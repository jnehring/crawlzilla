from crawler import CrawlerConfig
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
import argparse
import random
from concurrent.futures import ThreadPoolExecutor
from queue import Queue

import logging


# parse downloaded html files to extract clean text, languages and more.
class Parser:

    def __init__(self, config : CrawlerConfig):
        self.config = config
        self.parsed_folder = os.path.join(config.output_folder, config.parsed_folder)
        if not os.path.exists(self.parsed_folder):
            os.makedirs(self.parsed_folder)

        model_path = hf_hub_download(repo_id="facebook/fasttext-language-identification", filename="model.bin")
        self.language_identification = fasttext.load_model(model_path)

    def parse_line(self, line):
        try:
            source_data = json.loads(line)

            if source_data["status"] < 200 or source_data["status"] > 300:
                return

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

            # do not continue if there is no text and no language
            if len(languages) == 0:
                return

            # in case there is more than a single language we will skip this document
            # this can be implement smarter, e.g., skip if less then 90% is in one language
            if len(languages) > 1:
                return

            language = languages[0]

            # do not continue if the segment has the wrong language
            if language not in self.config.languages:
                return

            # put text together
            text = "\n".join([s["text"] for s in segments])

            parsed_data = {
                "url": source_data["url"],
                "language": language,
                "text": text
            }

            page_urls = {url for url in self.extract_urls(soup, source_data["url"])}
            parsed_data["parsed_urls"] = list(page_urls)

            return parsed_data

        except Exception as e:
            logging.exception(e)


    #     for line in gzip.open(infile, "rt"):



    # read a single json file that contains html data of many pages
    def parse_json(self, infile : str):

        queue = Queue()
        pool = ThreadPoolExecutor()

        outfile = os.path.join(self.parsed_folder, "tmp_" + os.path.basename(infile))

        def iterate_batches(reader, batch_size = 100):
            batch = []
            for line in reader:
                batch.append(line)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            yield batch


        urls = set()
        domains2languages = {}

        with gzip.open(infile, "rt") as reader:
            with gzip.open(outfile, "wt") as writer:
                for batch in iterate_batches(reader):
                    data = pool.map(self.parse_line, batch)

                    for parsed_data in data:
                        if parsed_data is None:
                            continue
                        json_data = json.dumps(parsed_data)
                        writer.write(json_data)
                        writer.write("\n")

                        for url in parsed_data["parsed_urls"]:
                            urls.add(url)

                        domain = urlparse(url).netloc
                        if domain not in domains2languages:
                            domains2languages[domain] = {}
                        language = parsed_data["language"]
                        if language not in domains2languages[domain]:
                            domains2languages[domain][language] = 1
                        else:
                            domains2languages[domain][language] += 1
                    
        return outfile, urls, domains2languages

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

            # params = href.find("?")
            # if params > 0:
            #     href = href[0:params]

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

if __name__ == "__main__":

    infile = "../output/html/00001.json.gz"
    config = CrawlerConfig()
    parser = Parser(config)
    outfile, urls, domains2languages = parser.parse_json(infile)
