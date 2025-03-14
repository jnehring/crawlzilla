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
    stats = {
        "language": language,
        "characters": 0,
        "sentences": 0,
        "words": 0,
        "urls": 0,
        "duplicates": 0,
    }

    dedups = set()

    print("create language " + language)
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

    stats["duplicates"] = 100 * stats["duplicates"] / stats["sentences"]

    print(f"processed {len(infiles)} files")
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

    # detect language
    languages = []
    if args.languages is None:
        def filter_languages(file):
            path = os.path.join(args.working_folder, file)
            return os.path.isdir(path) and file != 'seedurls'
        languages = [filter(filter_languages, os.listdir(args.working_folder))]
    else:
        if args.languages.find(',') > 0:
            languages = args.languages.split(",")
        else:
            languages = [args.languages]

    # process in the same or parallel threads
    if len(languages) ==  1:
        results = [create_language(languages[0], args)]
    else:
        with Pool() as pool:
            workers = [(language, args) for language in languages]
            results = pool.map(workers, create_language)


    # textual_output_folder = os.path.join(infolder, "textual_outputs/")
    # outfolder = os.path.join(infolder, "final_data")

    # files = os.listdir(textual_output_folder)
    # data = {}
    # for file in files:

    #     lang = get_lang(file)
    #     if lang not in data.keys():
    #         data[lang] = []
    #     data[lang].append(os.path.join(textual_output_folder, file))

    # stats = []
    # for lang, files in data.items():
    #     outfile = os.path.join(outfolder, lang + ".txt")
    #     _stats = create_language(lang, outfile, files)
    #     stats.append(_stats)

    # df = pd.DataFrame(stats).sort_values(by="words", ascending=False)
    # for c in ["characters", "sentences", "words", "urls"]:
    #     df[c] = df[c].apply(lambda x:f"{x:,}")

    # c = "duplicates"
    # df[c] = df[c].apply(lambda x:f"{x:.2f}%")

    # print(df)

    # n = count_lines(os.path.join(infolder, "downloaded_urls.txt"))
    # print(f"downloaded urls: {n:,}")

    # n = count_lines(os.path.join(infolder, "urls2download.txt"))
    # print(f"urls2download: {n:,}")

if __name__ == "__main__":
    main()