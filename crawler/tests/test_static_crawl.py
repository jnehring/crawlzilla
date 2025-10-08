
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
        seed_url = f"http://localhost:{self.port}/index.html"
        config = CrawlerConfig(
            output_folder=output_folder,
            languages=["eng_Latn"],
            seed_url=seed_url,
            start_fresh=True,
            dont_compress_outputs=True,
            download_sleep_time=0,
            num_rounds=2
        )
        
        start_crawler(config)

        # test round 1    
        round1_parsed = os.path.join(output_folder, 'parsed', '00001.json')
        lines = open(round1_parsed).readlines()

        # should contain 1 page only
        self.assertEqual(len(lines), 1)

        page1 = json.loads(lines[0])

        # first page should have these three fields
        self.assertTrue("url" in page1.keys())
        self.assertTrue("segments" in page1.keys())
        self.assertTrue("parsed_urls" in page1.keys())

        # the url should be the seed url
        self.assertEqual(page1['url'], seed_url)

        # should contain 73 distinct urls
        parsed_urls = page1['parsed_urls']
        self.assertEqual(len(set(parsed_urls)), 73)

        # test round 2
        round1_parsed = os.path.join(output_folder, 'parsed', '00002.json')
        lines = [json.loads(x) for x in open(round1_parsed).readlines()]
        self.assertEqual(len(lines), 72)

        urls = [x['url'] for x in lines]
        self.assertEqual(len(set(urls)), 72)


    def tearDown(self):
        self.server.shutdown()
        self.server.join()


if __name__ == '__main__':
    unittest.main()

