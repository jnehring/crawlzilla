# parse the output of the crawler / parser and write final data files
# also compute statistics 

import os
import gzip
import json
import pandas as pd
import nltk
import re
from tqdm import tqdm

output_folder = "../output/"

def response_codes():
    infolder = os.path.join(output_folder, "html")
    response_codes = {}
    infiles = os.listdir(infolder)
    pbar = tqdm(total=len(infiles))
    for infile in infiles:
        pbar.update(1)
        if infile[-7:] != "json.gz" or infile[0:4] == "tmp_":
            continue

        infile = os.path.join(infolder, infile)
        with gzip.open(infile, "rt") as reader:
            for line in reader:
                
                data = json.loads(line)
                rc = data["status"]
                if rc not in response_codes:
                    response_codes[rc] = 0

                response_codes[rc] += 1

    codes = sorted(list(response_codes.keys()))
    print()
    print("response codes")
    for code in codes:
        count = response_codes[code]
        if count > 10:
            print(f"{code}:\t{count}")

def read_parsed():
    infolder = os.path.join(output_folder, "parsed")
    outfolder = os.path.join(output_folder, "final_data/")

    if not os.path.exists(outfolder):
        os.mkdir(outfolder)

    writers = {}
    stats = {}
    dedups = {}

    try:
        infiles = os.listdir(infolder)
        pbar = tqdm(total=len(infiles))
        for infile in infiles:
            pbar.update(1)
            if infile[-7:] != "json.gz":
                continue

            infile = os.path.join(infolder, infile)

            with gzip.open(infile, "rt") as reader:
                for line in reader:
                    
                    data = json.loads(line)

                    language = data["language"]
                    if language not in writers:

                        outfile = os.path.join(outfolder, f"{language}.txt.gz")
                        writers[language] = gzip.open(outfile, "wt")

                        stats[language] = {
                            "characters": 0,
                            "sentences": 0,
                            "words": 0,
                            "urls": 0,
                            "duplicates": 0,
                        }

                        dedups[language] = set()

                    text = data["text"]

                    stats[language]["urls"] += 1

                    # remove multiple consecutive whitespaces
                    text = re.sub(r'\s+', ' ', text).strip()

                    # collect statistics
                    for l in text.split("\n"):
                        sent_text = nltk.sent_tokenize(l) # this gives us a list of sentences
                        stats[language]["sentences"] += len(sent_text)

                        for sent in sent_text:

                            # deduplicate
                            h = hash(sent)
                            if h in dedups[language]:
                                stats[language]["duplicates"] += 1
                                continue
                            dedups[language].add(h)

                            stats[language]["words"] += len(sent.split(" "))
                            stats[language]["characters"] += len(sent)
                            writers[language].write(sent)
                            writers[language].write("\n")

    finally:
        for writer in writers.values():
            writer.close()

    # collect statistics
    df = []
    total = {}
    for language in stats.keys():
        row = {
            "language": language
        }

        stats[language]["duplicates"] = 100 * stats[language]["duplicates"] / stats[language]["sentences"]

        if len(total) == 0:
            total = {stat:0 for stat in stats[language].keys()}
        for key, value in stats[language].items():
            row[key] = value
            total[key] += value
        df.append(row)

    total["language"] = "total"
    df.append(total)
    df = pd.DataFrame(df)
    print(df)


def main():

    read_parsed()
    response_codes()

if __name__ == "__main__":

    main()