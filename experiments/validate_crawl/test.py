import json
import os
import gzip
import sys
from bs4 import BeautifulSoup

path = "../../output_server/"
os.listdir(path)
lines = []
with gzip.open(os.path.join(path, "html", "00001.json.gz")) as reader:
    for line in reader:
        lines.append(json.loads(line))
        if len(lines) > 5:
            break


page = lines[1]

print(page["url"])

soup = BeautifulSoup(page["html"], "html.parser")


def parse_html(soup):

    print("hello world")
    return "hello world"

    for line in soup.get_text().split("\n"):
        line = line.strip()

        print(line)

        if len(line) == 0:
            continue

        # needs to have a minimum length
        if len(line) < 50:
            continue

        # needs to contain at least one sentence marks
        sentence_marks = ".,!?"
        counts = sum([line.count(x) for x in sentence_marks])

        if counts == 0:
            continue

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

        yield line

def extract_paragraphs_from_soup(soup):
    for line in soup.get_text().split("\n"):
        line = line.strip()

        print(line)

        if len(line) == 0:
            continue

        # needs to have a minimum length
        if len(line) < 50:
            continue

        # needs to contain at least one sentence marks
        sentence_marks = ".,!?"
        counts = sum([line.count(x) for x in sentence_marks])

        if counts == 0:
            continue

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

        yield line

soup = BeautifulSoup(page["html"], "html.parser")
extract_paragraphs_from_soup(soup)
#parse_html(soup)