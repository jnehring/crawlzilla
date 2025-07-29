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
from multiprocessing import Pool


def get_lang(file):
    file = file[file.find("_")+1:]
    file = file[0:file.find(".")]
    return file

def create_language(language, args):

    print("create " + language)
    language_folder = os.path.join(args.working_folder, language)

    to_folder = os.path.join(language_folder, "textual_outputs")

    stats = {
        "language": language,
        "characters": 0,
        "sentences": 0,
        "words": 0,
        "urls": 0,
        "duplicates": 0,
    }

    if os.path.exists(to_folder) and len(os.listdir(to_folder)) > 0:
        infiles = [os.path.join(to_folder, infile) for infile in os.listdir(to_folder)]

        outfile = os.path.join(args.working_folder, "final_output", f"final_data_{language}.txt")

        dedups = set()

        pbar = tqdm(total=len(infiles))

        with open(outfile, "w")  as writer:
            for file in infiles:
                for line in open(file):
                    h = hash(line)
                    if h in dedups:
                        continue

                    dedups.add(h)
                    writer.write(line)


                    stats["urls"] += 1

                    # collect statistics
                    for l in line.split("\n"):
                        sent_text = nltk.sent_tokenize(l) # this gives us a list of sentences
                        stats["sentences"] += len(sent_text)

                        for sent in sent_text:

                            # deduplicate
                            h = hash(sent)
                            if h in dedups:
                                stats["duplicates"] += 1
                                continue
                            dedups.add(h)

                            stats["words"] += len(sent.split(" "))
                            stats["characters"] += len(sent)

                pbar.update(1)


    stats["downloaded_urls"] = count_lines(os.path.join(language_folder, "downloaded_urls.txt"))
    stats["urls2download"] = count_lines(os.path.join(language_folder, "urls2download.txt"))
    stats["duplicates"] = 100 * stats["duplicates"] / stats["sentences"]
    return stats

def count_lines(infile):
    if not os.path.exists(infile):
        return 0
    else:
        return len(open(infile).readlines())

def parse_args():
    parser = argparse.ArgumentParser(
                        prog='Data Generator',
                        description='Generate data from crawls')
    
    parser.add_argument('--working_folder', default="../outputs/", type=str, help="Where is the data")
    parser.add_argument('--languages', default=None, type=str, help="Limit to certain languages")
    parser.add_argument('--report_location', default="../outputs/report.txt", type=str, help="Where to create the report.")

    return parser.parse_args()

def main():

    args = parse_args()
    try:
        nltk.data.find('tokenizers/punkt')
    except LookupError:
        nltk.download('punkt')

    final_output_folder = os.path.join(args.working_folder, "final_output")
    if not os.path.exists(final_output_folder):
        os.makedirs(final_output_folder)

    # detect language
    languages = []
    if args.languages is None:
        def filter_languages(file):
            path = os.path.join(args.working_folder, file)
            return os.path.isdir(path) and file != 'seedurls' and file != "final_output"
        languages = list(filter(filter_languages, os.listdir(args.working_folder)))
    else:
        if args.languages.find(',') > 0:
            languages = args.languages.split(",")
        else:
            languages = [args.languages]

    results = [create_language(language, args) for language in languages]

    # # process in the same or parallel threads
    # if len(languages) ==  1:
    #     results = [create_language(languages[0], args)]
    # else:
    #     with Pool() as pool:
    #         workers = [(language, args) for language in languages]
    #         results = pool.map(workers, create_language)

    results = pd.DataFrame(results)
    outfile = os.path.join(final_output_folder, "stats.csv")
    results.to_csv(outfile)
    print("wrote " + outfile)

    for c in ["characters", "sentences", "words", "urls", "downloaded_urls", "urls2download"]:
        results[c] = results[c].apply(lambda x:f"{x:,}")

    c = "duplicates"
    results[c] = results[c].apply(lambda x:f"{x:.2f}%")

    results = pd.DataFrame(results)
    outfile = os.path.join(final_output_folder, "stats.txt")
    with open(outfile, "w") as f:
        f.write(results.to_string())
    print("wrote " + outfile)

    sep = "-"*20
    for ix, row in results.iterrows():
        print(sep)
        print(row["language"])
        print(sep)
        for key, value in row.items():
            print(f"{key}:\t{value}")
        print(sep)

if __name__ == "__main__":
    main()