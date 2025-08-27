
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

class ServerThread(threading.Thread):
    def __init__(self, app, host="127.0.0.1", port=5000):
        super().__init__()
        self.server = make_server(host, port, app)
        self.ctx = app.app_context()
        self.ctx.push()

    def run(self):
        print("Starting server")
        self.server.serve_forever()

    def shutdown(self):
        print("Stopping server")
        self.server.shutdown()


class TestStaticCrawl(unittest.TestCase):

    def setUp(self):

        # Point to your downloaded static site folder
        static_dir = os.path.join(os.path.dirname(__file__), "assets/books.toscrape.com")
        self.app = Flask(
            __name__, 
            static_url_path='',
            static_folder=static_dir)
        
        self.port = 5000
        self.server = ServerThread(self.app)
        self.server.start()

    # def testStaticCrawlSinglePage(self):

    #     output_folder = "tests/assets/temp/static_crawl/"
    #     config = CrawlerConfig(
    #         output_folder=output_folder,
    #         languages=["eng_Latn"],
    #         #seed_url=f"http://localhost:{self.port}/index.html",
    #         seed_url=f"http://localhost:{self.port}/catalogue/the-past-never-ends_942/index.html",
    #         start_fresh=True,
    #         dont_compress_outputs=True,
    #         num_rounds=1
    #     )
        
    #     start_crawler(config)

    #     parsed = json.load(open(os.path.join(output_folder, "parsed", "00001.json")))
    #     self.assertEqual(len(parsed['parsed_urls']), 9)
    #     self.assertEqual(len(parsed['segments']), 1)

    def testStaticCrawlMultiPage(self):

        output_folder = "tests/assets/temp/static_crawl/"
        config = CrawlerConfig(
            output_folder=output_folder,
            languages=["eng_Latn"],
            seed_url=f"http://localhost:{self.port}/index.html",
            start_fresh=True,
            dont_compress_outputs=True,
        )
        
        start_crawler(config)
    


    def tearDown(self):
        self.server.shutdown()
        self.server.join()


if __name__ == '__main__':
    unittest.main()

