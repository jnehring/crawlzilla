
"""
Unit tests to crawl the static site.

Call it like this:

python -m tests.test_static_crawl
"""

from flask import Flask, send_from_directory
import os
import unittest
from bs4 import BeautifulSoup
from extract_text import HTML2Text
import threading
from werkzeug.serving import make_server
from crawler import CrawlerConfig, start_crawler
import json
from tests.util import ServerThread

class TestStaticCrawl(unittest.TestCase):

    def setUp(self):
        server, port = ServerThread.setup(os.path.join(os.path.dirname(__file__), "assets/books.toscrape.com"))
        self.server = server
        self.port = port

    def testStaticCrawlMultiPage(self):

        output_folder = "tests/assets/temp/static_crawl/"
        config = CrawlerConfig(
            output_folder=output_folder,
            languages=["eng_Latn"],
            seed_url=f"http://localhost:{self.port}/index.html",
            start_fresh=True,
            dont_compress_outputs=True,
            download_sleep_time=0
        )
        
        start_crawler(config)
    


    def tearDown(self):
        self.server.shutdown()
        self.server.join()


if __name__ == '__main__':
    unittest.main()

