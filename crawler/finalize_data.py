# parse the output of the crawler / parser and write final data files
# also compute statistics 

import os
import gzip
import json
import pandas as pd
import nltk
import re
from tqdm import tqdm
import argparse
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List
import shutil
import pyarrow.dataset as ds

# parse command line arguments
def parse_args():
    parser = argparse.ArgumentParser(
                        prog='Data Generator',
                        description='Generate data from crawls')
    
    parser.add_argument('--input_folder', default="../outputs/", type=str, help="folder in which the input data is stored. by default, this is ../outputs")
    parser.add_argument('--output_folder', default="../outputs/final_dataset", type=str, help="folder in which the output data is stored. by default, this is ../outputs/final_dataset")
    parser.add_argument('--languages', default=None, type=str, help="Limit to certain languages")
    parser.add_argument('--batch_size', default=500000, type=int, help="size of batches")

    return parser.parse_args()

# iterate over all data files and yield batches
def iterate_over_files(args):
    batch = []
    for folder in os.listdir(args.input_folder):
        if args.languages is not None and folder not in args.languages:
            print(f"skipping folder {os.path.join(args.input_folder, folder)}")
            continue
        
        dedub = []
        infolder = os.path.join(folder, "textual_outputs")
        infiles = os.listdir(os.path.join(args.input_folder, infolder))

        pbar = tqdm(total=len(infiles))
        language, script = folder.split("_")
        dedups = set()

        print(f"reading {len(infiles)} files from folder {folder}")

        for file in infiles:
            with open(os.path.join(args.input_folder, infolder, file)) as f:
                for line in f:
                    h = hash(line)
                    if h in dedups:
                        continue

                    dedups.add(h)

                    batch.append({
                        "text": line[0:-1],
                        "language": language,
                        "script": script
                    })


                    if len(batch) >= args.batch_size:
                        yield batch
                        batch = []

            pbar.update(1)

    if len(batch) > 0:
        yield batch

# initialize the parquet dataset
def create_parquet_schema(folder):

    if not os.path.exists(folder):
        os.makedirs(folder)

    schema = pa.schema([
        ("text", pa.large_string()),
        ("language", pa.dictionary(pa.int8(), pa.string())),  # efficient tiny codes
        ("script", pa.dictionary(pa.int8(), pa.string())),
    ])
    common_meta_path = os.path.join(folder, "_common_metadata")
    meta_path = os.path.join(folder, "_metadata")

    # Write dataset-level metadata that carries the schema
    pq.write_metadata(schema, common_meta_path)
    pq.write_metadata(schema, meta_path)

    return schema

# write one batch to parquet
def write_batch(folder : str, batch : List, batch_number : int):
    table = pa.table({
        "text": pa.array([row['text'] for row in batch], type=pa.large_string()),
        "language": pa.array([row['language'] for row in batch], type=pa.dictionary(pa.int8(), pa.string())),
        "script": pa.array([row['script'] for row in batch], type=pa.dictionary(pa.int8(), pa.string())),
    })

    writer = pq.ParquetWriter(
        os.path.join(folder, f"part-{batch_number}.parquet"),
        schema=table.schema,
        compression="zstd",
        use_dictionary=["language", "script"],
        write_statistics=True,
    )
    writer.write_table(table)

# read the dataset and compute statistics
def create_stats(args, dataset_folder):

    print('start computing statistics')
    dataset = ds.dataset(dataset_folder, format="parquet")

    total_rows = sum(fragment.metadata.num_rows for fragment in dataset.get_fragments())
    stats = {}
    pbar = tqdm(total=total_rows)
    for batch in dataset.to_batches():
        for row in batch.to_pylist():

            key = row['language'] + "_" + row['script']
            if key not in stats:
                stats[key] = {
                    "language": row['language'],
                    "script": row['script'],
                    "characters": 0,
                    "sentences": 0,
                    "words": 0,
                    "urls": 0,
                }
            for l in row['text'].split("\n"):
                sent_text = nltk.sent_tokenize(l) # this gives us a list of sentences
                stats[key]["sentences"] += len(sent_text)

                for sent in sent_text:
                    stats[key]["words"] += len(sent.split(" "))
                    stats[key]["characters"] += len(sent)
            
            pbar.update(1)

    report = []
    for s in stats.values():
        report.append(f"Language: {s['language']}")
        report.append(f"script: {s['script']}")
        report.append(f"words: {s['words']:,}")
        report.append(f"characters: {s['characters']:,}")
        report.append('---')

    report = "\n".join(report)

    print('Statistics')
    print(report)

    outfile = os.path.join(args.output_folder, "stats.txt")
    with open(outfile, "w") as f:
        f.write(report)
    print("wrote statistics to " + outfile)

def main():

    args = parse_args()

    if os.path.exists(args.output_folder):
        shutil.rmtree(args.output_folder)
    os.makedirs(args.output_folder)

    dataset_folder = os.path.join(args.output_folder, "dataset")
    schema = create_parquet_schema(dataset_folder)
    i = 0
    all_stats = {}
    for batch in iterate_over_files(args):
        write_batch(dataset_folder, batch, i)
        i += 1

    create_stats(args, dataset_folder)

if __name__ == "__main__":
    main()