import os
import gzip
import json
import pandas as pd

def main():

    infolder = "output/parsed"
    outfolder = "output/final_data/"

    if not os.path.exists(outfolder):
        os.mkdir(outfolder)

    for infile in os.listdir(infolder):
        if infile[-7:] != "json.gz":
            continue

        infile = os.path.join(infolder, infile)

        print("open " + infile)
        writers = {}

        stats = {}

        try:
            with gzip.open(infile, "rt") as reader:
                for line in reader:
                    
                    data = json.loads(line)

                    language = data["language"]
                    if language not in writers.keys():
                        outfile = os.path.join(outfolder, f"{language}.txt.gz")
                        writers[language] = gzip.open(outfile, "wt")

                        stats[language] = {
                            "num_characters": 0
                        }

                    writers[language].write(data["text"])
                    stats[language]["num_characters"] += len(data["text"])
                    writers[language].write("\n")

        finally:
            for writer in writers.values():
                writer.close()

    df = []
    for language in stats.keys():
        row = {
            "language": language
        }
        for key, value in stats[language].items():
            row[key] = value
        df.append(row)
    df = pd.DataFrame(df)
    print(df)

if __name__ == "__main__":

    main()