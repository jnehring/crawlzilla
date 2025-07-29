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
import logging

@dataclass
class CrawlerConfig:

    def __init__(self,
        output_folder : str = "../output",
        html_folder : str = "html",
        languages : List[str] = [
            'kin_Latn', 
        ],
        # languages : List[str] = [
        #     'swh_Latn', 
        #     'kin_Latn', 
        #     'yor_Latn', 
        #     'run_Latn', 
        #     'hau_Latn', 
        #     'amh_Latn', 
        #     'orm_Latn', 
        #     'lin_Latn',
        # ],
        seed_file : str = "assets/seedurls.txt.gz",
        parsed_folder : str = "parsed",
        round_size : int = 1000,
        num_rounds : int = -1,
        download_batch_size : int = 250,
        download_n_threads = 30,
        accept_content_types : List[str] = ["text/html"],
        request_timeout : int = 12,
        download_sleep_time : int = 0.1
        ):

        self.output_folder : str = output_folder
        self.html_folder : str = html_folder
        self.languages = languages
        self.seed_file : str = seed_file
        self.download_batch_size : int = download_batch_size
        self.download_n_threads : int = download_n_threads
        self.parsed_folder : str = parsed_folder
        self.round_size : int = round_size
        self.num_rounds : int = num_rounds
        self.accept_content_types : List[str] = accept_content_types
        self.request_timeout : int = request_timeout
        self.download_sleep_time : int = download_sleep_time
        self.text_folder : str = "textual_outputs"

        self.domain_language_filter_n = 10
        self.domain_language_filter_ratio = 0.2

# helper function to download a single url and convert the result to json
# it will be executed in parallel 
def download(args):
    url, config = args

    json_data = {
        "url": url,
    }

    try:
        r = requests.get(url, timeout=config.request_timeout)
        json_data["status"] = r.status_code

        if r.status_code >= 200 and r.status_code < 300: 

            json_data["headers"] = {key.lower():value.lower() for key, value in r.headers.items()}
            if "content-type" in json_data["headers"].keys():

                valid = False
                for ct in config.accept_content_types:
                    if json_data["headers"]["content-type"][0:len(ct)] == ct:
                        valid = True

                if valid:
                    r.encoding = r.apparent_encoding
                    json_data["html"] = r.text

    except Exception as e:
        json_data["status"] = -1
        json_data["error"] = str(e)

    contains_body = "html" in json_data.keys()
    json_data = json.dumps(json_data)
    time.sleep(config.download_sleep_time)
    return json_data, contains_body

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

        start_time = time.time()
        urls_with_html = 0
        for i in range(0, len(urls), self.config.download_batch_size):

            logging.info(f"download batch {i}-{i+self.config.download_batch_size}")
            batch = [(url, self.config, ) for url in urls[i:i+self.config.download_batch_size]]

            data = process_map(download, batch, max_workers=self.config.download_n_threads)

            for row in data:
                html, contains_body = row
                self.dump_writer.write(html)
                self.dump_writer.write("\n")
                if contains_body:
                    urls_with_html += 1
        self.dump_writer.close()

        t = time.time() - start_time
        logging.info(f"downloaded {urls_with_html:,} urls that contain html code in {t:.2f} seconds")

class DomainLanguageCounter:

    def __init__(self, config : CrawlerConfig):
        self.outfile = os.path.join(config.output_folder, "domain_language_counter.json")
        self.domains = {}
        self.domain_blacklist = set()
        self.config = config

    def add(self, domain2language):

        for domain in domain2language.keys():

            # if domain in self.domain_blacklist:
            #     return

            if domain not in self.domains.keys():
                self.domains[domain] = {}

            for language, count in domain2language[domain].items():

                if language not in self.domains[domain].keys():
                    self.domains[domain][language] = 0
                
                self.domains[domain][language] += count

                if sum(self.domains[domain].values()) >= self.config.domain_language_filter_n:
                    n1 = 0
                    n2 = 0
                    for language, count in self.domains[domain].items():
                        if language in self.config.languages:
                            n1 += 1
                        else:
                            n2 += 1
                    
                    if n2 > 0 and n1 / n2 < self.config.domain_language_filter_ratio:
                        self.domain_blacklist.add(domain)

    def is_blacklisted(self, url):
        domain = urlparse(url).netloc
        return domain in self.domain_blacklist

    def write(self):
        with open(self.outfile, "w") as f:
            data = {
                "blacklist": list(self.domain_blacklist),
                "domains": self.domains
            }
            f.write(json.dumps(data))

    def read_from_file(self):
        if os.path.exists(self.outfile):
            with open(self.outfile, "r") as f:
                data = json.load(f)
                self.domain_blacklist = set(data["blacklist"])
                self.domains = data["domains"]

    def filter_urls(self, urls):
        filtered_urls = []
        for url in urls:
            domain = urlparse(url).netloc
            if not self.is_blacklisted(domain):
                filtered_urls.append(url)
        return filtered_urls, len(urls) - len(filtered_urls)


class HTML2Text:

    def __init__(self):
        self.replace_consecutive_whitespace = re.compile(r'\s+')
        self.nodeTypes = set(["p", "span", "h1", "h2", "h3", "h4", "h5", "h6"])

    def iterate_nodes(self, parent):

        if not "contents" in parent.__dict__.keys():
            return

        for child in parent.contents:
            if child.name in self.nodeTypes:
                yield child
            else:
                for node in self.iterate_nodes(child):
                    yield node

    def clean_text(self, text):

        lines = []
        for line in text.split("\n"):
            line = line.strip()

            if len(line) == 0:
                continue

            if len(line) == 0:
                continue

            # needs to have a minimum length
            if len(line) < 50:
                continue

            # needs to contain at least one sentence marks
            sentence_marks = ".,!?"
            counts = sum([line.count(x) for x in sentence_marks])

            # if counts == 0:
            #     continue

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

            line = self.replace_consecutive_whitespace.sub(" ", line)

            lines.append(line)

        if len(lines) == 0:
            return None
        else:
            lines = list(set(lines))
            return "\n".join(lines)
            
    def extract_text(self, soup):
        texts = []
        for node in self.iterate_nodes(soup):
            text = self.clean_text(node.text)
            if text is not None:
                for line in text.split("\n"):
                    texts.append(line)
        return texts


# parse downloaded html files to extract clean text, languages and more.
class Parser:

    def __init__(self, config : CrawlerConfig):
        self.config = config
        self.parsed_folder = os.path.join(config.output_folder, config.parsed_folder)
        if not os.path.exists(self.parsed_folder):
            os.makedirs(self.parsed_folder)
        
        model_path = hf_hub_download(repo_id="facebook/fasttext-language-identification", filename="model.bin")
        self.language_identification = fasttext.load_model(model_path)
        self.html2text = HTML2Text()

    def parse_line(self, line):
        try:
            source_data = json.loads(line)

            if source_data["status"] < 200 or source_data["status"] > 300:
                return

            soup = BeautifulSoup(source_data["html"], "html.parser")
            segments = []
            for paragraph in self.html2text.extract_text(soup):
                lang = self.language_identification.predict(paragraph)[0][0]
                lang = lang[len("__label__"):]
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


    # read a single json file that contains html data of many pages
    def parse_json(self, infile : str):

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

        writers = {}
        try:
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

                            domain = urlparse(parsed_data["url"]).netloc
                            if domain not in domains2languages:
                                domains2languages[domain] = {}
                            language = parsed_data["language"]
                            if language not in domains2languages[domain]:
                                domains2languages[domain][language] = 1
                            else:
                                domains2languages[domain][language] += 1

                            if parsed_data["language"] not in writers.keys():
                                outfolder = os.path.join(self.config.output_folder, self.config.text_folder)
                                if not os.path.exists(outfolder):
                                    os.mkdir(outfolder)
                                
                                outfile_clean = os.path.basename(infile)
                                outfile_clean = outfile_clean[0:outfile_clean.find(".")] + "_" + parsed_data["language"] + ".txt"
                                outfile_clean = os.path.join(outfolder, outfile_clean)
                                writers[parsed_data["language"]] = open(outfile_clean, "w")
                            
                            writers[parsed_data["language"]].write(parsed_data["text"])
                            writers[parsed_data["language"]].write("\n")


        finally:
            for writer in writers.values():
                writer.close()
                        
                    
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

            if href[-1] == "/":
                href = href[0:-1]

            if href[0:4] != "http":
                continue

            urlp2 = urlparse(href)

            # use only internal links
            if urlp.netloc != urlp2.netloc:
                continue

            yield href

class URLStore:

    def __init__(self, file, start_urls = []):
        self.urls = start_urls
        self.file = file

    def write2file(self):
        self.urls = [u.replace("\n", "") for u in self.urls]
        self.urls = list(filter(lambda x:len(x.strip()) > 0, self.urls))
        with open(self.file, "w") as f:
            f.write("\n".join(self.urls))

    def read(self):
        if os.path.exists(self.file):
            with open(self.file, "r") as f:
                self.urls = f.readlines()

    def remove_urls(self, urls):
        self.urls = list(filter(lambda x:x not in urls, self.urls))

    def file_exists(self):
        return os.path.exists(self.file)

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
        downloaded_urls : DownloadedURLs,
        domain_language_counter : DomainLanguageCounter):

        self.config : CrawlerConfig = config
        self.html_store : HTMLStore = html_store
        self.parser : Parser = parser
        self.urls2download : URLs2Download = urls2download
        self.downloaded_urls : DownloadedURLs = downloaded_urls
        self.domain_language_counter : DomainLanguageCounter = domain_language_counter

    def round(self, num):
        filename = f"{num:05}.json.gz"
        html_file = os.path.join(self.config.output_folder, self.config.html_folder, filename)
        parse_file = os.path.join(self.config.output_folder, self.config.parsed_folder, filename)

        if os.path.exists(html_file) and os.path.exists(parse_file):
            logging.info(f"skip round {num}")
            return

        logging.info(f"start round {num}")
        logging.info(f"number of urls to download: {len(self.urls2download.urls):,}")
        logging.info(f"number of downloaded urls: {len(self.downloaded_urls.urls):,}")

        # download websites
        if not os.path.exists(html_file):

            tmp_file = os.path.join(self.config.output_folder, self.config.html_folder, "tmp_" + filename)

            self.html_store.init_round(tmp_file)

            urls2download = []

            for url in self.urls2download.urls:

                if len(urls2download) >= self.config.round_size:
                    break

                if url in self.downloaded_urls.urls or len(url.strip()) == 0:
                    continue

                # do not download blacklisted domains
                if self.domain_language_counter.is_blacklisted(url):
                    continue

                urls2download.append(url)

            self.html_store.download_urls(urls2download)

            self.urls2download.remove_urls(urls2download)
            self.urls2download.write2file()

            self.downloaded_urls.urls.extend(urls2download)
            self.downloaded_urls.write2file()

            os.rename(tmp_file, html_file)

        # parse data
        if not os.path.exists(parse_file):

            logging.info(f"parsing round {num}")
            tmp_file, new_urls, domains2languages = self.parser.parse_json(html_file)
            self.domain_language_counter.add(domains2languages)
            self.domain_language_counter.write()
            
            os.rename(tmp_file, parse_file)

            logging.info(f"extracted {len(new_urls):,} new urls")
            
            existing_urls = set(self.downloaded_urls.urls)
            urls2download = set(self.urls2download.urls)

            new_urls = list(new_urls)
            random.shuffle(new_urls)

            for url in new_urls:
                if url not in existing_urls and url not in urls2download:
                    self.urls2download.urls.append(url)
            self.urls2download.write2file()

def parse_args(config):
    parser = argparse.ArgumentParser(
                        prog='Crawler',
                        description='Crawl African Languages')
    
    parser.add_argument('--start_fresh', default=False, action="store_true", help="Set to True to remove all previously crawled data and start fresh.")
    parser.add_argument('--output_folder', default="../outputs", type=str, help="Where to store the output.")
    parser.add_argument('--seed_file', default="assets/seedurls.txt.gz", type=str, help="Seed file")
    parser.add_argument('--num_rounds', default=-1, type=int, help="How many rounds to download and parse.")
    parser.add_argument('--round_size', default=1000, type=int, help="How many URLs to download per round.")
    parser.add_argument('--download_batch_size', default=250, type=int, help="How many URLs to download per batch.")
    parser.add_argument('--download_n_threads', default=10, type=int, help="How many threads to parallel download data.")
    parser.add_argument('--language', default="../kin_Latn", type=str, help="Which language to use.")

    args = parser.parse_args()

    config.start_fresh = args.start_fresh
    config.output_folder = args.output_folder
    config.seed_file = args.seed_file
    config.num_rounds = args.num_rounds
    config.round_size = args.round_size
    config.download_batch_size = args.download_batch_size
    config.download_n_threads = args.download_n_threads
    config.language = [args.language]

    return args

def init_logging(config):
    # set up logging to file
    logging.basicConfig(
        filename=os.path.join(config.output_folder, 'log.log'),
        level=logging.INFO, 
        format= '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

def main():

    random.seed(0)

    config = CrawlerConfig()
    args = parse_args(config)

    init_logging(config)
    logging.info("Start Crawler")
    logging.info("Config: " + str(config.__dict__))

    if args.start_fresh:
        if os.path.exists(config.output_folder):
            shutil.rmtree(config.output_folder)

    # init all components
    html_store = HTMLStore(config)

    domain_language_counter : DomainLanguageCounter = DomainLanguageCounter(config)
    domain_language_counter.read_from_file()

    urls2download = URLs2Download([], config)
    if not urls2download.file_exists():
        urls = [f.replace("\n", "") for f in gzip.open(config.seed_file, "rt").readlines()]
        random.shuffle(urls)
        urls2download.urls = urls
    else:
        urls2download.read()

    downloaded_urls = DownloadedURLs(config)
    downloaded_urls.read()

    parser = Parser(config)
    crawler = Crawler(config, html_store, parser, urls2download, downloaded_urls, domain_language_counter)

    # start crawling
    round = 1
    while len(urls2download.urls) > 0:
        if config.num_rounds > 0 and config.num_rounds < round:
            break

        crawler.round(round)
        round += 1

if __name__ == "__main__":
    main()
