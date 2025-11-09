"""
Helper functions to extract clean text from HTML documents.
"""

import re
#import fitz
from bs4 import BeautifulSoup
import requests 

# this is not used because the data quality of pdfs was too low. might be extended in future.
def pdf2html(doc):
    """Convert PDF to HTML

    Args:
        input (_type_): input can be a string (path to local file on disk) or a requests.models.Response

    Returns:
        _type_: _description_
    """
    # if type(input) == str:
    #     doc = fitz.open(input)
    # elif type(input) == requests.models.Response:
    #     doc = fitz.open(stream=input.content, filetype="pdf")

    soup = BeautifulSoup("<html><head></head><body></body></html>", "html.parser")

    counter=0
    for page in doc:
        counter += 1
        if counter != 44:
            continue
        text = page.get_text("blocks")
        text = [t[4].strip() for t in text]

        segments = []
        for t in text:
            for t2 in t.split("\n\n"):
                t2 = re.sub(r'\n(?![•])', "", t2)
                segments.append(t2)

        for segment in segments:
            new_paragraph = soup.new_tag("p")
            new_paragraph.string = segment
            soup.body.append(new_paragraph)

    return soup


class HTML2Text:
    """Convert HTML code to text
    """
    def __init__(self):
        self.replace_consecutive_whitespace = re.compile(r'\s+')
        self.nodeTypes = set(["p", "span", "h1", "h2", "h3", "h4", "h5", "h6"])

    def iterate_nodes(self, parent):

        if not "contents" in parent.__dict__.keys():
            return

        for child in parent.contents:
            if child.name in self.nodeTypes:
                yield child
            else:
                for node in self.iterate_nodes(child):
                    yield node

    def clean_text(self, text):

        lines = []
        for line in text.split("\n"):
            line = line.strip()

            if len(line) == 0:
                continue

            if len(line) == 0:
                continue

            # needs to have a minimum length
            if len(line) < 50:
                continue

            # needs to contain at least one sentence marks
            sentence_marks = ".,!?"
            counts = sum([line.count(x) for x in sentence_marks])

            # needs to have a ratio of upper / lower characters
            lower = "abcdefghijklmnobqrstuvwxyz"
            upper = lower.upper()

            lower_ratio = sum(line.count(x) for x in lower) / len(line)
            upper_ratio = sum(line.count(x) for x in upper) / len(line)

            if lower_ratio > 0.95 or upper_ratio > 0.2:
                continue

            # should not end with ...
            needle = "..."
            if line[-3:] == needle:
                continue

            line = self.replace_consecutive_whitespace.sub(" ", line)

            lines.append(line)

        if len(lines) == 0:
            return None
        else:
            lines = list(set(lines))
            return "\n".join(lines)
            
    def extract_text(self, soup):
        texts = []
        for node in self.iterate_nodes(soup):
            text = self.clean_text(node.text)
            if text is not None:
                for line in text.split("\n"):
                    texts.append(line)
        return texts


