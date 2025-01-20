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

r = r'[^a-zA-Z0-9 -]'

lang_id_model = None
langid_lock = multiprocessing.Lock()

def lang_id(text : str):
    global lang_id_model
    with langid_lock:
        if lang_id_model is None:
            model_path = hf_hub_download(repo_id="facebook/fasttext-language-identification", filename="model.bin")
            lang_id_model = fasttext.load_model(model_path)
    x = lang_id_model.predict(text)
    return [y[0] for y in x]


lang_id("hello world")