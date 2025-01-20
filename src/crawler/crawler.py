import os
import logging
import re
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import shutil
import time
from tqdm import tqdm
import sys
import multiprocessing
import fasttext
from huggingface_hub import hf_hub_download
from cleaner import extract_paragraphs_from_soup
import json
import gzip
from diskcache import Cache
import sys
import numpy as np
from multiprocessing import Pool

lang_id_model = None
langid_lock = multiprocessing.Lock()

# language identification
def lang_id(text : str):
    global lang_id_model
    with langid_lock:
        if lang_id_model is None:
            model_path = hf_hub_download(repo_id="facebook/fasttext-language-identification", filename="model.bin")
            lang_id_model = fasttext.load_model(model_path)
    x = lang_id_model.predict(text)
    return [y[0] for y in x]

class CrawlerConfig:

    def __init__(self,
        crawler_name : str,
        url_prefix : str,
        seedurl : str,
        sleeptime : int = 1,
        num_rounds : int = 10,
        outfile_include_html : bool = False,
        outfile_include_text : bool = True,
        outfile_gzip_compress : bool = True,
        do_langid : bool = True,
        stop_langid : bool = False,
        stop_langid_num_urls : int = 10,
        filter_for_language : str = "__label__kin_Latn",
        datafolder : str = "../../data/crawls/",
        resume : bool = True):

        self.datafolder = datafolder
        self.crawler_name : str = crawler_name
        self.url_prefix : str = url_prefix
        self.seedurl : str = seedurl
        self.sleeptime : int = sleeptime
        self.num_rounds : int = num_rounds
        self.outfile_include_html : bool = outfile_include_html
        self.outfile_include_text : bool = outfile_include_text
        self.outfile_gzip_compress : bool = outfile_gzip_compress
        self.do_langid : bool = do_langid
        self.stop_langid : bool = stop_langid
        self.stop_langid_num_urls : int = stop_langid_num_urls
        self.filter_for_language = filter_for_language
        self.resume = resume

class Crawler:

    def __init__(self,
        config : CrawlerConfig
        ):

        self.config = config

        self.crawlfolder = os.path.join(config.datafolder, config.crawler_name)
        if not os.path.exists(self.crawlfolder):
            os.makedirs(self.crawlfolder)

        self.urls_to_crawl_file = os.path.join(self.crawlfolder, "urls2crawl.txt")
        self.parsed_urls_file = os.path.join(self.crawlfolder, "parsed_urls.txt")

        output_folder = os.path.join(self.crawlfolder, "output_folder")
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        working_folder = os.path.join(self.crawlfolder, "working_folder")
        if not os.path.exists(working_folder):
            os.makedirs(working_folder)

        self.round : int = 0

        self.cache : Cache = Cache(os.path.join(self.crawlfolder, "cache"))

        self.finished = False

        self.language_watcher = []

    # download a file from the working folder or load it from the cache
    def download(self, url : str):

        content = self.cache.get(url)
        if content is not None:
            return content
        else:
            try:
                logging.info(f"download {url}")
                r = requests.get(url, timeout=10)
                if r.status_code != 200:
                    logging.info(f"{url} returned status code {r.status_code}")
                content = r.content.decode("utf-8")
                self.cache.set(url, content)
                time.sleep(self.config.sleeptime)

            except Exception as e:
                logging.error(f"exception in {url}", str(e))

    # read all lines from an url file and clean the data
    def read_url_file(self, urls_to_crawl_file : str):
        if os.path.exists(urls_to_crawl_file):
            urls_to_crawl = filter(lambda x:len(x.strip())>0, open(urls_to_crawl_file).readlines())

            def clean(url):
                if url[-1] == "\n":
                    url = url[0:-1]
                return url
            urls_to_crawl = [clean(x) for x in urls_to_crawl]
            return urls_to_crawl
        else:
            return []
    
    # download all urls from urls2crawl file
    def fetch(self):
        urls_to_crawl = self.read_url_file(self.urls_to_crawl_file)
        logging.info(f"fetching {len(urls_to_crawl)} urls")
        #pbar = tqdm(total=len(urls_to_crawl))
        for url in urls_to_crawl:
            self.download(url)
        #    pbar.update(1)

    def get_outfile(self):
        filename = f"data_{self.round}.json"
        if self.config.outfile_gzip_compress:
            filename += ".gz"

        final_outfile = os.path.join(self.crawlfolder, f"{filename}")
        if self.config.resume and os.path.exists(final_outfile):
            logging.info(f"skip parsing of round {round} because file exists")

        temp_path = os.path.join(self.crawlfolder, f"temp_{filename}")
        return final_outfile, temp_path
        
    # parse all urls
    def parse(self):
        
        final_outfile, temp_path = self.get_outfile()
        if self.config.outfile_gzip_compress:
            outfile = gzip.open(temp_path, "wt")
        else:
            outfile = open(temp_path, "w")

        parsed_paragraphs = set()
        parsed_urls = set(self.read_url_file(self.parsed_urls_file))

        num_parsed = 0
        next_urls = set()
        for url in self.read_url_file(self.urls_to_crawl_file):

            html = self.cache.get(url)
            if html is None:
                logging.error(f"cannot find url {url} in cache")
                continue

            # parse html
            soup = BeautifulSoup(html, "html.parser")

            # extract links
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

                href = urljoin(url, href)
                if href[-1] == "/":
                    href = href[0:-1]
                needle = self.config.url_prefix
                if href[0:len(needle)] != needle:
                    continue

                if href in self.parsed_urls_file:
                    continue

                next_urls.add(href)

            # extract text    
            data = {
                "url": url,
            }

            if self.config.outfile_include_html:
                data["html"] = html
            
            languages = []
            if self.config.outfile_include_text:
                data["text"] = []

                for paragraph in extract_paragraphs_from_soup(soup):
                    h = hash(paragraph)
                    if h in parsed_paragraphs:
                        continue
                    parsed_paragraphs.add(h)

                    paragraph_data = {
                        "text": paragraph
                    }

                    # language detection
                    if self.config.do_langid:
                        language, confidence = lang_id(paragraph)
                        paragraph_data["langid"] =  {
                            "lang": language,
                            "confidence": confidence
                        }
                        languages.append(language)

                    data["text"].append(paragraph_data)

                # apply the language filter
                valid = True
                if self.config.do_langid and self.config.filter_for_language is not None and len(languages)>0:
                    values, counts = np.unique(languages, return_counts=True)
                    counts = counts.astype(float) / counts.sum()
                    i = np.where(values==self.config.filter_for_language)[0]

                    if len(i) == 0 or values[i] != self.config.filter_for_language or counts[i] < 0.5:
                        self.language_watcher.append(0)
                        logging.info(f"{url} is filtered out because it does not contain enough Kinyarwanda")
                        valid = False
                    self.language_watcher.append(1)

                    if len(self.language_watcher) == self.config.stop_langid_num_urls and \
                        sum(self.language_watcher) / len(self.language_watcher) < 0.5:
                        logging.info(f"stop crawling {self.config.crawler_name} because 50% of the pages did not contain language {self.config.filter_for_language}")
                        self.finished = True
                        return
            if valid:
                outfile.write(json.dumps(data))
                outfile.write("\n")
            
        with open(self.urls_to_crawl_file, "w") as f:
            for url in next_urls:
                f.write(url)
                f.write("\n")

        with open(self.parsed_urls_file, "a") as f:
            for url in next_urls:
                f.write(url)
                f.write("\n")

        os.rename(temp_path, final_outfile)

        logging.info(f"finished parsing, parsed {num_parsed} urls")

        self.finished = len(next_urls)==0

    def crawl(self):

        # create urls2crawl file with seed if necessary
        if not self.config.resume or not os.path.exists(self.urls_to_crawl_file):
            with open(self.urls_to_crawl_file, "w") as f:
                f.write(self.config.seedurl)

        if not self.config.resume and os.path.exists(self.parsed_urls_file):
            os.remove(self.parsed_urls_file)

        msg = [f"starting crawljob \"{self.config.crawler_name}\". Config:"]
        for key, value in self.config.__dict__.items():
            msg.append(f"- {key}: {value}")
        logging.info("\n".join(msg))

        for i in range(self.config.num_rounds):
            logging.info(f"starting round {i+1}/{self.config.num_rounds}")

            self.round = i

            final_outfile, _ = self.get_outfile()
            if self.config.resume and os.path.exists(final_outfile):
                logging.info(f"skip round {self.round} because outfile {final_outfile} exists")

            self.fetch()
            self.parse()

            if self.finished:
                break
        logging.info("crawling finished")

def run_async(config: CrawlerConfig):
    crawler = Crawler(config)
    crawler.crawl()

def init_logging():
    # set up logging to file
    logging.basicConfig(
        filename='log.log',
        level=logging.INFO, 
        format= '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

def start_single_crawl(crawler_name, url_prefix, seedurl, **kwargs):
    config = CrawlerConfig(crawler_name, url_prefix, seedurl, **kwargs)
    crawler = Crawler(config)
    crawler.crawl()

def crawl_multiple():
    urls = ['inyarwanda.com', 'kigalitoday.com', 'intyoza.com', 'umunsi.com', 'muhabura.rw', 'tebyan.net', 'igihe.com', 'rwandamagazine.com', 'umuseke.rw', 'igishushanyo.com']
    configs = [CrawlerConfig(url, "https://" + url, "https://" + url) for url in urls]

    with Pool(4) as pool:
        print(pool.map(run_async, configs))

if __name__ == "__main__":

    init_logging()

    # start a single crawl
    # start_single_crawl("https://inyarwanda.com")

    start_single_crawl("bbc.com", "http://bbc.com/", "http://bbc.com/", outfile_gzip_compress=False)