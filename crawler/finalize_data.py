# parse the output of the crawler / parser and write final data files
# also compute statistics 

import os
import gzip
import json
import pandas as pd
import nltk
import re
from tqdm import tqdm

def get_lang(file):
    file = file[file.find("_")+1:]
    file = file[0:file.find(".")]
    return file

def create_language(language, outfile, infiles):
    stats = {
        "language": language,
        "characters": 0,
        "sentences": 0,
        "words": 0,
        "urls": 0,
        "duplicates": 0,
    }

    dedups = set()

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

    return stats

def create_final_data():

    infolder = "../output/textual_outputs/"
    outfolder = "../output/final_data/"

    if not os.path.exists(outfolder):
        os.makedirs(outfolder)

    files = os.listdir(infolder)
    data = {}
    for file in files:

        lang = get_lang(file)
        if lang not in data.keys():
            data[lang] = []
        data[lang].append(os.path.join(infolder, file))

    stats = []
    for lang, files in data.items():
        outfile = os.path.join(outfolder, lang + ".txt")
        _stats = create_language(lang, outfile, files)
        stats.append(_stats)

    df = pd.DataFrame(stats)
    for c in ["characters", "sentences", "words", "urls"]:
        df[c] = df[c].apply(lambda x:f"{x:,}")

    c = "duplicates"
    df[c] = df[c].apply(lambda x:f"{x:.2f}%")

    print(df)

if __name__ == "__main__":
    create_final_data()