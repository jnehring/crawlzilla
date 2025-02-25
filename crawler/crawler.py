from dataclasses import dataclass
import os
import pandas as pd
from abc import ABC, abstractmethod
from typing import List
import requests
import re
import json
from bs4 import BeautifulSoup
import fasttext
from huggingface_hub import hf_hub_download
import gzip
from tqdm.contrib.concurrent import process_map
import time
from urllib.parse import urljoin, urlparse
import sys
import shutil

@dataclass
class CrawlerConfig:

    def __init__(self,
        output_folder : str = "output",
        html_folder : str = "html",
        languages : List[str] = ['swh_Latn', 'kin_Latn', 'yor_Latn', 'run_Latn', 'hau_Latn', 'amh_Latn', 'orm_Latn', 'lin_Latn'],
        seed_file : str = "assets/seedurls.txt",
        parsed_folder : str = "parsed",
        round_size : int = 500,
        download_batch_size : int = 250,
        download_n_threads = 10
        ):

        self.output_folder : str = output_folder
        self.html_folder : str = html_folder
        self.languages = languages
        self.seed_file : str = seed_file
        self.download_batch_size : int = download_batch_size
        self.download_n_threads : int = download_n_threads
        self.parsed_folder : str = parsed_folder
        self.round_size = round_size

# helper function to download a single url and convert the result to json
# it will be executed in parallel 
def download(url):
    json_data = {
        "url": url,
    }

    try:
        r = requests.get(url, timeout=15)
        json_data["status"] = r.status_code
        if r.status_code >= 200 and r.status_code < 300: 
            json_data["html"] = r.text
    except Exception as e:
        json_data["status"] = -1
        json_data["error"] = str(e)

    json_data = json.dumps(json_data)
    time.sleep(1)
    return json_data

# codes related to downloading and storing html data 
class HTMLStore:

    def __init__(self, config : CrawlerConfig):

        self.config : CrawlerConfig = config
        self.html_folder = os.path.join(config.output_folder, config.html_folder)
        if not os.path.exists(self.html_folder):
            os.makedirs(self.html_folder)

        self.current_round = 1
        self.crawled_urls = set()

        self.dump_writer = None

    # open the file writer in the beginning of each round
    def init_round(self, dump_file):
        self.dump_writer = gzip.open(dump_file, "wt")

    # download a list of urls in parallel in multiple batches
    def download_urls(self, urls : List[str]):

        for i in range(0, len(urls), self.config.download_batch_size):

            print(f"download batch {i}-{i+self.config.download_batch_size}")
            batch = urls[i:i+self.config.download_batch_size]

            data = process_map(download, batch, max_workers=self.config.download_n_threads)

            for row in data:
                self.dump_writer.write(row)
                self.dump_writer.write("\n")
        self.dump_writer.close()

# parse downloaded html files to extract clean text, languages and more.
class Parser:

    def __init__(self, config : CrawlerConfig):
        self.config = config
        self.parsed_folder = os.path.join(config.output_folder, config.parsed_folder)
        if not os.path.exists(self.parsed_folder):
            os.makedirs(self.parsed_folder)

        model_path = hf_hub_download(repo_id="facebook/fasttext-language-identification", filename="model.bin")
        self.language_identification = fasttext.load_model(model_path)

    # read a single json file that contains html data of many pages
    def parse_json(self, infile : str):
        outfile = os.path.join(self.parsed_folder, "tmp_" + os.path.basename(infile))
        urls = set()
        with gzip.open(outfile, "wt") as writer:
            for line in gzip.open(infile, "rt"):

                try:
                    source_data = json.loads(line)

                    if source_data["status"] < 200 or source_data["status"] > 300:
                        continue

                    soup = BeautifulSoup(source_data["html"], "html.parser")
                    segments = []
                    for paragraph in self.extract_paragraphs_from_soup(soup):
                        lang = self.language_identification.predict(paragraph)[0][0]
                        lang = lang[len("__labal__"):]
                        segment = {
                            "text": paragraph,
                            "language": lang
                        }
                        segments.append(segment)

                    languages = list(set([s["language"] for s in segments]))

                    if len(languages) > 1:
                        continue

                    text = "\n".join([s["text"] for s in segments])

                    language = None
                    if len(languages) == 1:
                        language = languages[0]

                    if language not in self.config.languages:
                        continue

                    parsed_data = {
                        "url": source_data["url"],
                        "language": language,
                        "text": text
                    }

                    page_urls = {url for url in self.extract_urls(soup, source_data["url"])}
                    parsed_data["parsed_urls"] = list(page_urls)

                    parsed_data = json.dumps(parsed_data)
                    writer.write(parsed_data)
                    writer.write("\n")

                    for url in page_urls:
                        urls.add(url)
                except Exception as e:
                    print("exception " + str(e))
        return outfile, urls

    # get all urls from a website
    def extract_urls(self, soup : BeautifulSoup, source_url : str):

        urlp = urlparse(source_url)
        basename = f"{urlp.scheme}://{urlp.netloc}{urlp.path}"

        for a in soup.find_all("a"):
            if not a.has_attr("href"):
                continue
            href = a["href"]

            if href == "#":
                continue

            i = href.find("#")
            if i>=0:
                href = href[0:i]
            if len(href) == 0:
                continue

            if (len(href) < 4 or href[0:4] != "http") and href [0] == "/":
                href = urljoin(basename, href)

            if href[-1] == "/":
                href = href[0:-1]

            if href[0:4] != "http":
                continue

            urlp2 = urlparse(href)

            # use only internal links
            if urlp.netloc != urlp2.netloc:
                continue

            yield href


    def extract_paragraphs(self, infile):
        html = open(infile)
        html = html.readlines()[1:]
        html = "\n".join(html)

        soup = BeautifulSoup(html, "html.parser")
        for p in self.extract_paragraphs_from_soup(soup):
            yield p

    def extract_paragraphs_from_soup(self, soup):

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

class URLStore:

    def __init__(self, file, start_urls = []):
        self.urls = start_urls
        self.file = file

    def write2file(self):
        with open(self.file, "w") as f:
            f.write("\n".join(self.urls))

    def read(self):
        with open(self.file, "r") as f:
            self.urls = f.readlines()

    def remove_urls(self, urls):
        self.urls = list(filter(lambda x:x not in urls, self.urls))

class URLs2Download(URLStore):

    def __init__(self, seed_urls, config):
        outfile = os.path.join(config.output_folder, "urls2download.txt")
        super().__init__(outfile, seed_urls)


class DownloadedURLs(URLStore):

    def __init__(self, config):
        outfile = os.path.join(config.output_folder, "downloaded_urls.txt")
        super().__init__(outfile)


class Crawler:

    def __init__(self,
        config : CrawlerConfig,
        html_store : HTMLStore,
        parser : Parser,
        urls2download : URLs2Download,
        downloaded_urls : DownloadedURLs):

        self.config : CrawlerConfig = config
        self.html_store : HTMLStore = html_store
        self.parser : Parser = parser
        self.urls2download : URLs2Download = urls2download
        self.downloaded_urls : DownloadedURLs = downloaded_urls

    def round(self, num):
        filename = f"{num:05}.json.gz"
        html_file = os.path.join(self.config.output_folder, self.config.html_folder, filename)

        # download websites
        if not os.path.exists(html_file):
            print(f"downloading round {num}")

            tmp_file = os.path.join(self.config.output_folder, self.config.html_folder, "tmp_" + filename)

            self.html_store.init_round(tmp_file)

            urls2parse = []

            for url in self.urls2download.urls:
                if len(urls2parse) > self.config.round_size:
                    break
                    
                if url in self.downloaded_urls.urls:
                    continue

                urls2parse.append(url)

            self.html_store.download_urls(urls2parse)

            self.urls2download.remove_urls(urls2parse)
            self.urls2download.write2file()

            self.downloaded_urls.urls.extend(urls2parse)
            self.downloaded_urls.write2file()

            os.rename(tmp_file, html_file)


        # parse data
        parse_file = os.path.join(self.config.output_folder, self.config.parsed_folder, filename)
        if not os.path.exists(parse_file):

            print(f"parsing round {num}")
            tmp_file, new_urls = self.parser.parse_json(html_file)
            os.rename(tmp_file, parse_file)


def main():

    config = CrawlerConfig()
    html_store = HTMLStore(config)

    start_fresh = False

    if start_fresh:

        for folder in [config.html_folder, config.parsed_folder]:
            folder = os.path.join(config.output_folder, folder)
            if os.path.exists(folder):
                shutil.rmtree(folder)
                os.mkdir(folder)

        urls2download = open(config.seed_file).readlines()
        urls2download = [f.replace("\n", "") for f in urls2download]
        urls2download = URLs2Download(urls2download, config)
        urls2download.write2file()

        downloaded_urls = DownloadedURLs(config)
    else:
        urls2download = URLs2Download([], config)
        urls2download.read()
        downloaded_urls = DownloadedURLs(config)
        downloaded_urls.read()

    parser = Parser(config)

    crawler = Crawler(config, html_store, parser, urls2download, downloaded_urls)

    round = 1
    while len(urls2download.urls) > 0:
        crawler.round(round)
        round += 1

if __name__ == "__main__":
    main()