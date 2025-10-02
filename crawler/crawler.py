from dataclasses import dataclass
import os
from abc import ABC, abstractmethod
from typing import List, Dict
import requests
import re
import json
from bs4 import BeautifulSoup
import fasttext
from huggingface_hub import hf_hub_download
import gzip
from tqdm.contrib.concurrent import process_map
from tqdm import tqdm
import time
from urllib.parse import urljoin, urlparse
import sys
import shutil
import argparse
import random
from concurrent.futures import ThreadPoolExecutor
import logging
import numpy as np
import glob
import fitz
import traceback
import copy
from collections import defaultdict
from extract_text import HTML2Text

@dataclass
class CrawlerConfig:
    
    def __init__(self,
        languages : List[str] = [],
        seed_file : str = "",
        output_folder : str = "../output",
        html_folder : str = "html",
        parsed_folder : str = "parsed",
        round_size : int = 1000,
        num_rounds : int = -1,
        download_batch_size : int = 250,
        download_n_threads = 30,
        accept_content_types : Dict[str,str] = {
            "text/html": "html",
            # "application/pdf": "pdf"
        },
        request_timeout : int = 12,
        download_sleep_time : int = 1,
        filter_for_languages : bool = True,
        log_level : str = "info",
        delete_parsed : bool = False,
        delete_html : bool = False,
        dont_compress_outputs : bool = False,
        seed_url : str = None,
        start_fresh : bool = False,
        request_headers : Dict[str,str] = {
            "User-Agent": "Crawlzilla/1.0)",
            "Accept": "text/html"
        }):

        self.output_folder : str = output_folder
        self.html_folder : str = html_folder
        self.languages = languages
        self.seed_file : str = seed_file
        self.download_batch_size : int = download_batch_size
        self.download_n_threads : int = download_n_threads
        self.parsed_folder : str = parsed_folder
        self.round_size : int = round_size
        self.num_rounds : int = num_rounds
        self.accept_content_types : Dict[str,str] = accept_content_types
        self.request_timeout : int = request_timeout
        self.download_sleep_time : int = download_sleep_time
        self.text_folder : str = "textual_outputs"
        self.filter_for_languages = filter_for_languages
        self.log_level : str = log_level
        self.seed_url : str = seed_url
        self.dont_compress_outputs : bool = dont_compress_outputs
        self.start_fresh : bool = start_fresh
        self.delete_parsed : bool = delete_parsed
        self.delete_html : bool = delete_html
        self.request_headers : Dict[str,str] = request_headers
        
    def clone(self):
        return copy.deepcopy(self)

# helper function to download a single url and convert the result to json
# it will be executed in parallel 
def download(args):
    url, config, pbar = args

    json_data = {
        "url": url,
    }

    try:
        r = requests.get(url, headers=config.request_headers, timeout=config.request_timeout)
        json_data["status"] = r.status_code

        if r.status_code >= 200 and r.status_code < 300: 

            json_data["headers"] = {key.lower():value.lower() for key, value in r.headers.items()}
            if "content-type" in json_data["headers"].keys():

                valid = False
                parser_type = None
                for ct in config.accept_content_types.keys():
                    if json_data["headers"]["content-type"][0:len(ct)] == ct:
                        valid = True
                        parser_type = config.accept_content_types[ct]

                if valid:
                    r.encoding = r.apparent_encoding

                    if parser_type == "html":
                        json_data["html"] = r.text

                    elif parser_type == "pdf":
                        
                        json_data["text"] = text

                else:
                    logging.debug(f"skip {url} because of undesired content-type header {ct}")
            else:
                logging.debug(f"skip {url} because it does not specify a content-type header")

    except Exception as e:
        json_data["status"] = -1
        json_data["error"] = str(e)
        logging.debug(e)

    contains_body = "html" in json_data.keys()
    json_data = json.dumps(json_data)

    if config.download_sleep_time > 0:
        time.sleep(config.download_sleep_time)

    pbar.update(1)
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

        if self.config.dont_compress_outputs:
            self.dump_writer = open(dump_file, "w")
        else:
            self.dump_writer = gzip.open(dump_file, "wt")


    # batch urls for friendly download
    # we never download two urls from the same domain in the same batch
    def batch_urls(self, urls, batch_size : int = 250):
        # extract the domain from the urls
        domains = []
        for url in urls:
            parsed = urlparse(url)
            domain = parsed.netloc
            # remove "www." if you want the bare domain
            if domain.startswith("www."):
                domain = domain[4:]
            domains.append(domain)

        domain_list = defaultdict(list)
        for url, domain in zip(urls, domains):
            domain_list[domain].append(url)

        # sort urls to batches
        max_n = max([len(value) for value in domain_list.values()])
        batches = []
        for i in range(max_n):
            batch = []
            
            for domain, url_list in domain_list.items():
                if i < len(url_list):
                    batch.append(url_list[i])

                if len(batch) >= batch_size:
                    batches.append(batch)
                    batch = []

            if len(batch) > 0:
                batches.append(batch)

        return batches
    
    # download a list of urls in parallel in multiple batches
    def download_urls(self, urls : List[str]):

        start_time = time.time()
        urls_with_html = 0

        # batch urls for friendly download
        # we never download two urls from the same domain in the same batch
        batches = self.batch_urls(urls, batch_size=self.config.download_batch_size)
        pbar = tqdm(total=len(urls))
        for i in range(len(batches)):

            # logging.info(f"download batch {i} with {len(batches[i])} urls")
            batch = [(url, self.config, pbar) for url in batches[i]]

            with ThreadPoolExecutor(max_workers=self.config.download_n_threads) as executor:
                #data = process_map(download, batch, max_workers=self.config.download_n_threads)
                data = list(executor.map(download, batch))

            for row in data:
                html, contains_body = row
                self.dump_writer.write(html)
                self.dump_writer.write("\n")
                if contains_body:
                    urls_with_html += 1
        self.dump_writer.close()

        t = time.time() - start_time
        logging.info(f"downloaded {urls_with_html:,} urls that contain html code in {t:.2f} seconds")


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

    def parse_segments(self, paragraphs : List[str], url : str):
        
        segments = []
        for paragraph in paragraphs:
            lang = self.language_identification.predict(paragraph)[0][0]
            lang = lang[len("__label__"):]
            segment = {
                "text": paragraph,
                "language": lang
            }
            segments.append(segment)
        #logging.debug(f"extracted segments from {source_data['url']}: {segments}")

        languages = list([s["language"] for s in segments])
        #logging.debug(f"detected languages from {source_data['url']}: {languages}")

        # do not continue if there is no text and no language
        if len(languages) > 0:
        
            # if there are multiple languages in the document, only accept documents that have more than 80% in the desired languages
            languages, counts = np.unique(languages, return_counts=True)
            count_dict = {languages[j] : counts[j] for j in range(len(counts))}

            # check if it has a desired language
            has_desired_language = False
            for lang in self.config.languages:
                if lang in count_dict.keys():
                    has_desired_language = True
                    break

            if has_desired_language:
                desired_language_count = np.sum([count_dict[lang] for lang in self.config.languages])
                frac = desired_language_count / np.sum(list(count_dict.values()))
            else:
                frac = 0.0
            
            if self.config.filter_for_languages and frac < 0.8:
                logging.debug(f"skip {url} because less than 80% of the data amount to the target language. Languages: {count_dict}")
                return []
        
        segments = list(filter(lambda x:x["language"] in self.config.languages, segments))
        return segments
        
    def parse_html(self, source_data):
        soup = BeautifulSoup(source_data["html"], "html.parser")

        segments = list(self.html2text.extract_text(soup))
        segments = self.parse_segments(segments, source_data["url"])

        parsed_data = {
            "url": source_data["url"],
            "segments": segments
        }

        page_urls = {url for url in self.extract_urls(soup, source_data["url"])}
        parsed_data["parsed_urls"] = list(page_urls)

        return parsed_data
    
    def parse_text(self, source_data):
        texts = []
        for block in source_data["text"]:
            texts = self.html2text.clean_text(block)
            if texts is None:
                continue
            print(texts)
            print("-")

        sys.exit(0)

    def parse_line(self, line):
        try:
            source_data = json.loads(line)

            logging.debug(f"parse {source_data['url']}")
            if source_data["status"] < 200 or source_data["status"] > 300:
                logging.debug(f"skip parsing of {source_data['url']} because of http status code {source_data['status']}")
                return

            if "html" in source_data.keys():
                return self.parse_html(source_data)
            elif "text" in source_data.keys():
                return self.parse_text(source_data)


        except Exception as e:
            logging.exception(e)


    # read a single json file that contains html data of many pages
    def parse_json(self, infile : str):

        logging.debug("start parsing " + infile)
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
            if infile[-3:] == ".gz":
                reader = gzip.open(infile, "rt")
            else:
                reader = open(infile, "r")

            if self.config.dont_compress_outputs:
                writer = open(outfile, "w")
            else:
                writer = gzip.open(outfile, "wt")

            try:
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

                        for segment in parsed_data["segments"]:

                            if segment["language"] not in writers.keys():
                                outfolder = os.path.join(self.config.output_folder, self.config.text_folder)
                                if not os.path.exists(outfolder):
                                    os.mkdir(outfolder)
                                
                                outfile_clean = os.path.basename(infile)
                                outfile_clean = outfile_clean[0:outfile_clean.find(".")] + "_" + segment["language"] + ".txt"
                                outfile_clean = os.path.join(outfolder, outfile_clean)
                                writers[segment["language"]] = open(outfile_clean, "w")
                            
                            writers[segment["language"]].write(segment["text"])
                            writers[segment["language"]].write("\n")

                        logging.debug(f"wrote {len(parsed_data['segments'])} segments from {parsed_data['url']}")
            finally:
                writer.close()

        finally:
            for writer in writers.values():
                writer.close()
            if reader is not None:
                reader.close()
                        
                    
        return outfile, urls

    # get all urls from a website
    def extract_urls(self, soup : BeautifulSoup, source_url : str):

        urlp = urlparse(source_url)

        base_href = soup.find('base', href=True)
        if base_href:
            base_url = urljoin(source_url, base_href['href']) # Resolve base tag's href if it's relative
        else:
            base_url = source_url


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

            if href=="./":
                continue

            # change relative urls to absolute urls
            href = urljoin(base_url, href)

            if href[-1] == "/":
                href = href[0:-1]

            if href[0:4] != "http":
                continue

            urlp2 = urlparse(href)

            # use only internal links
            if urlp.netloc != urlp2.netloc:
                continue

            yield href

# helper class to keep a list of urls in memory and serialize / unserialize it from a json file
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

# the urls that we need to download
class URLs2Download(URLStore):

    def __init__(self, seed_urls, config):
        outfile = os.path.join(config.output_folder, "urls2download.txt")
        super().__init__(outfile, seed_urls)

# the urls that we already downloaded
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
        filename = f"{num:05}.json"

        if not self.config.dont_compress_outputs:
            filename += ".gz"

        html_file = os.path.join(self.config.output_folder, self.config.html_folder, filename)
        parse_file = os.path.join(self.config.output_folder, self.config.parsed_folder, filename)
        textual_files = glob.glob(os.path.join(self.config.output_folder, self.config.text_folder, f"{num:05}*"))

        if len(textual_files) > 0:
            logging.info(f"skip round {num} because of existing output files: {textual_files}")
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
            tmp_file, new_urls = self.parser.parse_json(html_file)
            
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

        # cleanup
        if self.config.delete_html:
            os.remove(html_file)
            logging.debug(f"deleted file {html_file} because delete_html=True")
        if self.config.delete_parsed:
            os.remove(parse_file)
            logging.debug(f"deleted file {parse_file} because delete_parsed=True")

def parse_args(config):
    parser = argparse.ArgumentParser(
                        prog='Crawler',
                        description='Crawl African Languages')
    
    parser.add_argument('--seed_file', default=None, type=str, help="Seed file")
    parser.add_argument('--seed_url', required=False, type=str, help="Start with a single seed url. This overwrites --seed_file. It is used for debugging.")
    parser.add_argument('--language', required=True, type=str, help="Which language to use. This is the ISO_639-3 code for the language and the ISO 15924 code for the script, e.g. kin_Latn for Kinyarwanda in Latin script. You can crawl multiple languages together by separating them with a comma, e.g., kin_Latn, run_Latn")
    parser.add_argument('--start_fresh', default=False, action="store_true", help="Set to True to remove all previously crawled data and start fresh.")
    parser.add_argument('--output_folder', default="../outputs", type=str, help="Where to store the output.")
    parser.add_argument('--num_rounds', default=-1, type=int, help="How many rounds to download and parse. Set to -1 run until there are no more URLs.")
    parser.add_argument('--round_size', default=1000, type=int, help="How many URLs to download per round.")
    parser.add_argument('--download_batch_size', default=250, type=int, help="How many URLs to download per batch.")
    parser.add_argument('--download_n_threads', default=10, type=int, help="How many threads to parallel download data.")
    parser.add_argument('--log_level', default="info", type=str, choices=["info", "debug"], help="Adjust the logging level")
    parser.add_argument('--delete_parsed', default=False, action="store_true", help="Delete the parsed data when the round has ended.")
    parser.add_argument('--delete_html', default=False, action="store_true", help="Delete the html data when the round has ended.")
    parser.add_argument('--dont_compress_outputs', default=False, action="store_true", help="GZip compress the output files")

    args = parser.parse_args()

    if args.seed_file is None and args.seed_url is None:
        raise Exception("please provide either --seed_file or seed_url")

    config.start_fresh = args.start_fresh
    config.output_folder = args.output_folder
    config.seed_file = args.seed_file
    config.num_rounds = args.num_rounds
    config.round_size = args.round_size
    config.download_batch_size = args.download_batch_size
    config.download_n_threads = args.download_n_threads
    config.languages = args.language.strip().split(",")
    config.log_level = args.log_level
    config.seed_url = args.seed_url
    config.delete_parsed = args.delete_parsed
    config.delete_html = args.delete_html
    config.dont_compress_outputs = args.dont_compress_outputs

    return args

def init_logging(config):
    # set up logging to file

    if config.log_level.lower() == "info":
        log_level = logging.INFO
    elif config.log_level.lower() == "debug":
        log_level = logging.DEBUG
    else:
        raise Exception(f"unknown log level \"{config.log_level}\"" )

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("log.log"),
            logging.StreamHandler()
        ]
    )

def start_crawler(config):

    random.seed(0)

    if config.start_fresh:
        if os.path.exists(config.output_folder):
            shutil.rmtree(config.output_folder)

    if not os.path.exists(config.output_folder):
        os.makedirs(config.output_folder)

    init_logging(config)
    logging.info("Start Crawler")
    logging.info("Config: " + str(config.__dict__))

    # init all components
    html_store = HTMLStore(config)

    urls2download = URLs2Download([], config)
    if not urls2download.file_exists():

        if config.seed_url is not None:
            urls = [config.seed_url.strip()]
        else:
            if config.seed_file[-3:] == ".gz":
                urls = gzip.open(config.seed_file, "rt").readlines()
            else:
                urls = open(config.seed_file, "r").readlines()

            urls = [f.replace("\n", "") for f in urls]
            urls = list(filter(lambda url : len(url.strip()) > 0, urls))

            logging.info(f"initialize crawler with {len(urls)} seed urls")
        random.shuffle(urls)
        urls2download.urls = urls
    else:
        urls2download.read()

    downloaded_urls = DownloadedURLs(config)
    downloaded_urls.read()

    parser = Parser(config)
    crawler = Crawler(config, html_store, parser, urls2download, downloaded_urls)

    # start crawling
    round = 1

    while len(urls2download.urls) > 0:
        if config.num_rounds > 0 and config.num_rounds < round:
            break

        crawler.round(round)
        round += 1

if __name__ == "__main__":

    config = CrawlerConfig()
    parse_args(config)
    start_crawler(config)
