
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

    def setup(web_dir, port=5000):
        app = Flask(
            __name__, 
            static_url_path='',
            static_folder=web_dir)
        

        server = ServerThread(app, port=port)
        server.start()
        return server, port
