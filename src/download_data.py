import json
import tqdm
from urllib.request import urlretrieve
import os
import logging
import gzip

def create_logger():
    logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
    rootLogger = logging.getLogger()

    fileHandler = logging.FileHandler("app.log")
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)
    rootLogger.setLevel(logging.INFO)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)

def process_cc_file(infile, writer, datafile_name):
    count=0
    for content, url in parse_cc_file(infile):
        data = {"url": url, "cc_datafile": datafile_name, "content": content}
        writer.write(json.dumps(data))
        writer.write("\n")
        count += 1
    logging.info(f"wrote {count} documents")

def parse_cc_file(infile):
    uri_needle = "WARC-Target-URI"

    i=0
    tld = ".co.rw"

    MODE_HEADER = 1
    MODE_CONTENT = 3
    tld_match = False

    mode = MODE_CONTENT
    current_url = None
    current_page = []
    with gzip.open(infile, 'rt') as f:
        for line in f:
            line = line[0:-1]
            if mode == MODE_HEADER:
                if line[0:len(uri_needle)] == uri_needle and tld in line:
                    tld_match = True
                    current_url = line[len("WARC-Target-URI: "):]
                if len(line.strip()) == 0:
                    mode = MODE_CONTENT
                    current_page = []

            elif mode == MODE_CONTENT:
                if line == "WARC/1.0":
                    mode = MODE_HEADER
                    tld_match = False
                    if len(current_page) > 0:
                        yield current_page, current_url
                        current_page = []
                
                if tld_match:
                    current_page.append(line)

    if len(current_page) > 0:
        yield current_page, current_url

def run(skip=0, limit=2):
    create_logger()
    logging.info(f"start with skip={skip}, limit={limit}")

    search_results_file = "../data/search_results.json"
    count=0
    download_dir = "../data/download_dir"
    outfile = "../data/data.json"

    if not os.path.exists(download_dir):
        os.mkdir(download_dir)

    with open(outfile, "w") as writer:
        for line in open(search_results_file):
            count+=1
            if count==limit:
                break

            cc_datafile = json.loads(line)
            url = "https://data.commoncrawl.org/"
            url += cc_datafile["filename"].replace(".warc.gz", ".warc.wet.gz").replace("/warc/", "/wet/")
            filename = url[url.rfind("/")+1:]
            local_file = os.path.join(download_dir, filename)
            if os.path.exists(local_file) or skip>count:
                logging.info(f"{count} skip download of {filename}")
            else:
                logging.info(f"{count} download {filename}")
                try:
                    urlretrieve(url, local_file)
                except Exception as e:
                    logging.exception(e)

            try:
                process_cc_file(local_file, writer, filename)
            except Exception as e:
                logging.exception(e)

if __name__ == "__main__":
    run()


#https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-42/segments/1727944253525.17/wet/CC-MAIN-20241008044807-20241008074807-00162.warc.gz    
#https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-42/segments/1727944253525.17/warc/CC-MAIN-20241008044807-20241008074807-00162.warc.wet.gz
