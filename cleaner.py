from bs4 import BeautifulSoup
import justext
import os
from tqdm import tqdm
import numpy as np
import nltk
from tqdm import tqdm
from multiprocessing import Pool
import re

def extract_paragraphs(infile):
    html = open(infile)
    html = html.readlines()[1:]
    html = "\n".join(html)

    soup = BeautifulSoup(html, "html.parser")

    for line in soup.get_text().split("\n"):
        line = line.strip()

        if len(line) == 0:
            continue

        # needs to have a minimum length
        if len(line) < 50:
            continue

        # needs to contain at least one sentence marks
        sentence_marks = ".,!?"
        counts = sum([line.count(x) for x in sentence_marks])

        if counts == 0:
            continue

        # needs to have a ratio of upper / lower characters
        lower = "abcdefghijklmnobqrstuvwxyz"
        upper = lower.upper()

        lower_ratio = sum(line.count(x) for x in lower) / len(line)
        upper_ratio = sum(line.count(x) for x in upper) / len(line)

        if lower_ratio > 0.95 or upper_ratio > 0.2:
            continue

        # should not end with ...
        needle = "..."
        if line[-3:] == needle:
            continue

        yield line

def run():

    outfile = "igihe/data.txt"
    lines = set()
    n_lines = 0
    n_duplicates = 0
    infolder = "igihe/output_folder/"

    sentence_splitter = r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s'

    n_files = 0
    with open(outfile, "w") as writer:
        files = os.listdir(infolder)
        n_files = len(files)
        pbar = tqdm(total=len(files))
        for infile in files:
            infile = os.path.join(infolder, infile)
            for paragraph in extract_paragraphs(infile):
                h = hash(paragraph)

                if h in lines:
                    n_duplicates += 1
                    continue
                n_lines += 1
                lines.add(h)

                for sentence in re.split(sentence_splitter, paragraph):
                    if len(sentence) < 10:
                        continue
                    h = hash(sentence)
                    if sentence in lines:
                        n_duplicates += 1
                        continue
                    
                    writer.write(sentence)
                    writer.write("\n")
                    lines.add(h)
            pbar.update(1)
        
    print(f"processed {n_files} files, wrote {n_lines} lines, skipped {n_duplicates} duplicates")
        
if __name__ == "__main__":
    run()