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

r = r'[^a-zA-Z0-9 -]'
def url2path(url, working_folder):
    filename = re.sub(r, "_", url)
    path = os.path.join(working_folder, filename)
    return path

def download(url, working_folder):
    logging.info(f"download {url}")
    path = url2path(url, working_folder)
    if not os.path.exists(path):
        try:
            content = requests.get(url, timeout=10).content
            content = content.decode("utf-8")
            with open(path, "w") as f:
                f.write(url)
                f.write("\n")
                f.write(content)
            
            time.sleep(1)

        except Exception as e:
            logging.exception(f"exception in {url}", e)

def fetch(crawlfolder):

    urls_to_crawl_file = os.path.join(crawlfolder, "urls2crawl.txt")
    urls_to_crawl = set()
    if os.path.exists(urls_to_crawl_file):
        urls_to_crawl = filter(lambda x:len(x.strip())>0, open(urls_to_crawl_file).readlines())

        def clean(url):
            if url[-1] == "\n":
                url = url[0:-1]
            return url
        urls_to_crawl = [clean(x) for x in urls_to_crawl]


    output_folder = os.path.join(crawlfolder, "output_folder")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    working_folder = os.path.join(crawlfolder, "working_folder")
    
    if not os.path.exists(working_folder):
        os.makedirs(working_folder)

    pbar = tqdm(total=len(urls_to_crawl))
    for url in urls_to_crawl:
        download(url, working_folder)
        pbar.update(1)

def create_seed(crawlfolder, url):
    os.makedirs(crawlfolder)
    urls_to_crawl_file = os.path.join(crawlfolder, "urls2crawl.txt")
    with open(urls_to_crawl_file, "w") as f:
        f.write(url)

def parse(crawlfolder):
    working_folder = os.path.join(crawlfolder, "working_folder")
    output_folder = os.path.join(crawlfolder, "output_folder")
    
    urls2crawl = []
    for file in os.listdir(working_folder):
        content = open(os.path.join(working_folder, file)).readlines()
        url = content[0]
        html = "\n".join(content[1:])
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a"):
            if not a.has_attr("href"):
                continue
            href = a["href"]

            if href == "#":
                continue

            href = urljoin(url, href)
            if href[-1] == "/":
                href = href[0:-1]
            needle = "https://igihe.com"
            if href[0:len(needle)] != needle:
                continue

            path = url2path(href, working_folder)
            if os.path.exists(path):
                continue
            path = url2path(href, output_folder)
            if os.path.exists(path):
                continue

            urls2crawl.append(href)

        shutil.move(os.path.join(working_folder, file), os.path.join(output_folder, file))

    urls_to_crawl_file = os.path.join(crawlfolder, "urls2crawl.txt")
    with open(urls_to_crawl_file, "w") as f:
        for url in urls2crawl:
            f.write(url)
            f.write("\n")

    return len(urls2crawl)>0

def init_logging():
    # set up logging to file
    logging.basicConfig(
        filename='log.log',
        level=logging.INFO, 
        format= '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

if __name__ == "__main__":

    init_logging()
    crawlfolder = "igihe"
    seed = "https://igihe.com/index.php"
    logging.info(f"start crawling to {crawlfolder}")
    
    if not os.path.exists(crawlfolder):
        create_seed(crawlfolder, seed)

    num_rounds=10
    for i in range(num_rounds):
        logging.info(f"round {i+1}/{num_rounds}")
        fetch(crawlfolder)
        if not parse(crawlfolder):
            break

    logging.info("crawling finished")
