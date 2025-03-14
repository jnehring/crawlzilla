from crawler import download, CrawlerConfig
from tqdm.contrib.concurrent import process_map
from abc import ABC, abstractmethod

def download_batch(urls, config, n_workers):
    batch = []
    process_map(download, batch, max_workers=n_workers)

def main():

    urls2download = open("../output_server/urls2download.txt").readlines()
    urls2download = [x.replace("\n", "") for x in urls2download]
    urls2download = list(filter(lambda x:len(x.strip()) > 0, urls2download))

    config = CrawlerConfig()


if __name__ == "__main__":

    main()