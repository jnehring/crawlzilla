import unittest
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from crawler import HTMLStore, start_crawler, CrawlerConfig
from tests.util import ServerThread
import os
from warcio.archiveiterator import ArchiveIterator

import logging
logging.basicConfig(level=logging.ERROR)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

class TestHTMLStore(unittest.TestCase):

    def test_batch_urls(self):

        config = CrawlerConfig()

        store = HTMLStore(config)
        urls = ['https://portalnews.news/page26', 'https://mysite.net/page36', 'https://demo.co/page36', 'https://sample.io/page44', 'https://webpage.biz/page10', 'https://example.com/page20', 'https://example.com/page39', 'https://sample.io/page48', 'https://mysite.net/page42', 'https://randompage.info/page48', 'https://www.testsite.org/page', 'https://serviceapp.tech/page27', 'https://mysite.net/page49', 'https://serviceapp.tech/page40', 'https://example.com/page49', 'https://funzone.tv/page23', 'https://www.musicworld.fm/page', 'https://portalnews.news/page41', 'https://portalnews.news/page33', 'https://sportsarena.pro/page48', 'https://serviceapp.tech/page47', 'https://portalnews.news/page16', 'https://datahub.ai/page3', 'https://travelers.club/page40', 'https://travelers.club/page2', 'https://sportsarena.pro/page12', 'https://mysite.net/page8', 'https://serviceapp.tech/page3', 'https://sportsarena.pro/page36', 'https://example.com/page8', 'https://cloudbase.app/page18', 'https://example.com/page38', 'https://webpage.biz/page4', 'https://cloudbase.app/page36', 'https://portalnews.news/page4', 'https://sample.io/page50', 'https://serviceapp.tech/page20', 'https://example.com/page43', 'https://sample.io/page6', 'https://datahub.ai/page4', 'https://cloudbase.app/page27', 'https://coolstuff.dev/page40', 'https://serviceapp.tech/page45', 'https://cloudbase.app/page8', 'https://mysite.net/page40', 'https://coolstuff.dev/page22', 'https://serviceapp.tech/page47', 'https://www.myblog.me/page', 'https://funzone.tv/page45', 'https://sample.io/page8', 'https://sample.io/page2', 'https://webpage.biz/page35', 'https://portalnews.news/page15', 'https://portalnews.news/page50', 'https://sportsarena.pro/page32', 'https://example.com/page20', 'https://travelers.club/page25', 'https://portalnews.news/page28', 'https://sample.io/page3', 'https://travelers.club/page20', 'https://travelers.club/page12', 'https://demo.co/page21', 'https://coolstuff.dev/page9', 'https://coolstuff.dev/page39', 'https://randompage.info/page25', 'https://demo.co/page24', 'https://sample.io/page18', 'https://datahub.ai/page49', 'https://travelers.club/page16', 'https://demo.co/page12', 'https://mysite.net/page7', 'https://datahub.ai/page30', 'https://mysite.net/page38', 'https://portalnews.news/page49', 'https://coolstuff.dev/page6', 'https://example.com/page6', 'https://sample.io/page23', 'https://sportsarena.pro/page33', 'https://www.eduplace.edu/page', 'https://randompage.info/page25', 'https://funzone.tv/page5', 'https://portalnews.news/page16', 'https://onlinestore.shop/page12', 'https://cloudbase.app/page13', 'https://funzone.tv/page12', 'https://portalnews.news/page48', 'https://travelers.club/page38', 'https://mysite.net/page29', 'https://travelers.club/page7', 'https://serviceapp.tech/page32', 'https://onlinestore.shop/page8', 'https://onlinestore.shop/page50', 'https://datahub.ai/page43', 'https://portalnews.news/page31', 'https://cloudbase.app/page4', 'https://onlinestore.shop/page33', 'https://demo.co/page17', 'https://example.com/page26', 'https://www.govsite.gov/page', 'https://travelers.club/page42']
        batches = store.batch_urls(urls, batch_size=5)

        for batch in batches:
            self.assertTrue(len(batch) <= 5)

            domains = []
            for url in batch:
                parsed = urlparse(url)
                domain = parsed.netloc
                if domain.startswith("www."):
                    domain = domain[4:]
                self.assertFalse(domain in domains)
                domains.append(domain)

    def test_warc(self):

        server, port = ServerThread.setup(os.path.join(os.path.dirname(__file__), "assets/books.toscrape.com"), port=5001)
        output_folder = "tests/assets/temp/static_crawl/"
        config = CrawlerConfig(
            output_folder=output_folder,
            languages=["eng_Latn"],
            seed_url=f"http://localhost:{port}/index.html",
            start_fresh=True,
            dont_compress_outputs=True,
            download_sleep_time=0,
            warc_output=True,
            num_rounds=2,
            round_size=10,
        )
        
        start_crawler(config)
        server.shutdown()
        server.join()

        def read_urls(warcfile):
            urls = []
            with open(warcfile, 'rb') as stream:
                for record in ArchiveIterator(stream):
                    if record.rec_type == 'response':
                        urls.append(record.rec_headers.get_header('WARC-Target-URI'))
            return urls
        
        urls = read_urls('tests/assets/temp/static_crawl/warc/00001.warc.gz')
        self.assertEqual(len(urls), 1)

        urls = read_urls('tests/assets/temp/static_crawl/warc/00002.warc.gz')
        self.assertEqual(len(urls), 10)