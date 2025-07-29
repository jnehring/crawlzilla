import os
import gzip
import json
from tqdm.contrib.concurrent import process_map  

def parse_file(infile):
    for line in gzip.open(infile, "rt"):
        line = json.load(line)
        print(line)

def main():

    infolder = "../outputs/kin_Latn/parsed"
    files = [os.path.join(infolder, infile) for infile in os.listdir(infolder)]
    results = [process_map(parse_file, files)]

if __name__ == "__main__":

    main()