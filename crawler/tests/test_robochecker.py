
"""
Unit tests for robochecker. Call it like this:

python -m unittest tests.test_robochecker
"""

from flask import Flask, send_from_directory
import os
import unittest
from bs4 import BeautifulSoup
from extract_text import HTML2Text
import threading
from werkzeug.serving import make_server
import json
from robochecks import RobotsCache, RobotsChecker

import logging
logging.basicConfig(level=logging.WARNING)

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


class TestRobochecker(unittest.TestCase):

    def test_robochecker(self):
        # Point to your downloaded static site folder
        static_dir = os.path.join(os.path.dirname(__file__), "assets/robochecker")
        self.app = Flask(
            __name__, 
            static_url_path='',
            static_folder=static_dir)
        
        self.port = 5000
        self.server = ServerThread(self.app)
        self.server.start()


        cache_file = "tests/assets/temp/robots_cache.pkl"
        if os.path.exists(cache_file):
            os.remove(cache_file)

        rc = RobotsChecker(cache_file=cache_file)

        cache_key = f"http://localhost:{self.port}"
        can_fetch_url = f"http://localhost:{self.port}/index.html"
        cannot_fetch_url = f"http://localhost:{self.port}/no-crawl/test.html"

        self.assertIsNone(rc.cache.get_robots_txt(cache_key))

        self.assertEqual(rc.get_crawl_sleep_delay(can_fetch_url), 5)
    
        user_agent_forbidden = 'Crawlzilla-0.5'
        user_agent_allowed = 'Crawlzilla-1.0'
        
        # fetching is allowed for whitelisted url and the good user agent
        self.assertTrue(rc.check_robots(can_fetch_url, user_agent_allowed))

        # check disallow crawling by url
        result = rc.check_robots(cannot_fetch_url, user_agent_allowed)
        self.assertFalse(result['can_fetch'])

        # check disallow crawling by user agent
        result = rc.check_robots(can_fetch_url, user_agent_forbidden)
        self.assertFalse(result['can_fetch'])

        # check the cache is populated
        self.assertIsNotNone(rc.cache.get_robots_txt(cache_key))

        # test that the cache is still populated after restart
        rc = RobotsChecker(cache_file=cache_file)
        self.assertIsNotNone(rc.cache.get_robots_txt(cache_key))

        self.server.shutdown()
        self.server.join()

    @unittest.skip("This test is for debugging only")
    def test_parallel_download(self): 
        
        cache_file = "tests/assets/temp/robots_cache.pkl"
        if os.path.exists(cache_file):
            os.remove(cache_file)

        rc = RobotsChecker(cache_file=cache_file)
        urls = ['https://umuryango.rw/imyidagaduro/imikino/article/chelsea-ishobora-gufatirwa-ibihano-bikomeye-na-fifa', 
                'https://www.bbc.com/gahuza/amakuru-36435571', 
                'https://www.bbc.com/gahuza/amakuru-38333703', 
                'https://www.bbc.com/gahuza/amakuru-51986913', 
                'https://www.irmct.org/rw/amakuru/22-12-12-perezida-gatti-santana-yabonanye-na-bwana-antonio-guterres-umunyamabanga-mukuru']
        can_fetch, cannot_fetch = rc.can_fetch_multiple_urls(urls)

if __name__ == '__main__':
    unittest.main()

