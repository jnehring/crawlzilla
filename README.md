# Crawlzilla

This is the Crawlzilla, a software program to crawl websites and generate LLM training data for low resource lanugages.

Features

1. High speed.
2. Friendly crawling - Crawlzilla respects robots.txt, implements waiting times and does not crawl aggressively.
3. Easy to setup - Crawlzilla does not require any server infrastructure to run. It can easily run on a single laptop.

## Table of Contents

1. [Getting started](#getting-started)
    * [Installation](#installation)
    * [Starting the crawler](#starting-the-crawler)
    * [Crawlzilla`s command line parameters](#crawlzilla-s-command-line-parameters)
2. [Technical Documentation](#technical-documentation)
    * [Overview of the application](#overview-of-the-application)
    * [Output Folder Structure](#output-folder-structure)
3. [Crawling concepts](#crawling-concepts)
    * [Resume functionality](#resume-functionality)
    * [WARC Output](#warc-output)
    * [Run unit tests](#run-unit-tests)

## Getting started

### Installation

**Install Git, Python**
**Clone this repository**

```
git clone ...
```

**Create virtual environment**

```
cd crawler
python -m pip install venv
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt 
```

**Prepare output directory**

```
mkdir -p ../seeds/
```

**Prepare seed urls**

```
echo "https://www.kigalitoday.com/" >> ../seeds/seeds_kin_Latn.txt
gzip ../seeds/seeds_kin_Latn.txt
```

### Starting the crawler

**Standard configuration for a crawler**

```
python3 crawler.py \
    --output_folder ../outputs/kin_Latn \
    --language kin_Latn,run_Latn \
    --seed_file ../seeds/seeds_kin_Latn.txt.gz \
    --delete_parsed \
    --delete_html
```

**Debug crawl for a single page**

```
python3 crawler.py \
    --output_folder ../outputs/kin_Latn \
    --language kin_Latn \
    --seed_url https://www.kigalitoday.com \
    --log_level debug \
    --dont_compress_outputs \
    --num_rounds 1
```

### Crawlzilla`s command line parameters

```
$ python crawler.py -h
usage: Crawler [-h] [--seed_file SEED_FILE] [--seed_url SEED_URL]
               --language LANGUAGE [--start_fresh]
               [--output_folder OUTPUT_FOLDER] [--num_rounds NUM_ROUNDS]
               [--round_size ROUND_SIZE]
               [--download_batch_size DOWNLOAD_BATCH_SIZE]
               [--download_n_threads DOWNLOAD_N_THREADS]
               [--log_level {info,debug}] [--delete_parsed] [--delete_html]
               [--dont_compress_outputs] [--warc_output]

Crawl African Languages

options:
  -h, --help            show this help message and exit
  --seed_file SEED_FILE
                        Seed file
  --seed_url SEED_URL   Start with a single seed url. This overwrites
                        --seed_file. It is used for debugging.
  --language LANGUAGE   Which language to use. This is the ISO_639-3 code for
                        the language and the ISO 15924 code for the script, e.g.
                        kin_Latn for Kinyarwanda in Latin script. You can crawl
                        multiple languages together by separating them with a
                        comma, e.g., kin_Latn, run_Latn
  --start_fresh         Set to True to remove all previously crawled data from
                        the output folder and start fresh.
  --output_folder OUTPUT_FOLDER
                        Where to store the output.
  --num_rounds NUM_ROUNDS
                        How many rounds to download and parse. Set to -1 run
                        until there are no more URLs.
  --round_size ROUND_SIZE
                        How many URLs to download per round.
  --download_batch_size DOWNLOAD_BATCH_SIZE
                        How many URLs to download per batch.
  --download_n_threads DOWNLOAD_N_THREADS
                        How many threads to parallel download data.
  --log_level {info,debug}
                        Adjust the logging level
  --delete_parsed       Delete the parsed data when the round has ended.
  --delete_html         Delete the html data when the round has ended.
  --dont_compress_outputs
                        GZip compress the output files.
  --warc_output         Write WARC files in addition to the normal JSON files.
```

## Technical Documentation

### Overview of the application

<img src="/images/flowchart.drawio.png" />

The image shows the workflow of the application. All this logic is implemented in `crawler/crawler.py`.

1. The crawling starts with a list of seed urls that we retrieved from the CommonCrawl.
2. The next step is the crawling loop. The crawling loop operates in rounds. Each round first downloads a certain number of URLs, e.g. 2000 URLs per round. The crawling loop is implemented in function `main()` and the method `round` in class `Crawler`. 
3. The list URLs2Download stores all URLs that the system should crawl and is initialized with the SeedURLs. The crawler serializes this disk to the file `urls2download.txt`. This logic is implemented by the class `URLs2Download`.
4. The fetch step downloads all URLs of a round. The class `HTMLStore` implements this logic.
5. The parser step contains multiple subtstep. First, it extracts the clean text using the class `HTML2Text`.
6. The parser also performs [language detection with FastText](https://huggingface.co/facebook/fasttext-language-identification).
7. The parser also extracts all URLs from the HTML codes in method `extract_urls` of class `Parser`.
8. The list `DownloadedURLs` keeps track of all URLs that have already been crawled to avoid duplicate crawling. It acts like a filter in step 7 - extract links. It is implemented in class `DownloadedURLs` and serialized to disk in file `urls2download.txt`.
9. The extracted URLs are fed back to URLs2Download and the crawling loop begins the next round.
10. When the URLs2Crawl list is empty or the configured number of rounds is reached, the finalize step generates the final output data and statistics. The finalize step is implemented outside the crawler in `crawler/finalize_data.py`.

### Output Folder Structure

Crawlzilla creates the following output files.

```
outputs
└── kin_Latn                           # One output folder per language / script
    ├── downloaded_urls.txt            # The list of downloaded urls to avoid downloading the same URL twice
    ├── urls2download.txt              # It contains one file for each round.
    ├── html                           # The results of the fetch phase, mostly HTML code. It contains one file for each round
    │   ├── 00001.json.gz              # It contains one file for each round.
    │   └── 00002.json.gz
    ├── parsed                         # The results of the parsing step.
    │   ├── 00001.json.gz              # It contains one file for each round.
    │   └── 00002.json.gz
    ├── textual_outputs                # The generated textual data of each round.
    │   └── 00001_kin_Latn.txt         # It contains one file for each round.
    └── domain_language_counter.json   # This is currently not used
 ```


## Crawling concepts

### Resume functionality

If the crawling stops, you can resume it by simply restarting it. This is a very important feature because if you already crawled for one month and the crawler crashes, you do not want to start from scratch. However, during development, this can be confusing. E.g., when the urls2download list is empty, the crawler will not crawl anything. During debugging, the `--start_fresh` option disables the resume function. But be careful: `--start_fresh` deletes the crawling output folder. 

### WARC Output

Crawlzilla uses its own output format. However, it can also generate its outputs in the WARC format. WARC is an industry standard and many other tools (e.g., the [OSCAR project](https://oscar-project.org/)] process WARC files. The following examples shows how to use the `warc_output` parameter. Also, it uses the `delete_parsed` and `delete_html` parameters so it generates WARC only.

```
python3 crawler.py \
    --output_folder ../outputs/kin_Latn \
    --language kin_Latn \
    --seed_url https://www.kigalitoday.com \
    --num_rounds 5 \
    --delete_parsed \
    --delete_html \
    --warc_output
```

### Run unit tests

Crawlzilla implements some unit tests. When you change the code, you can run unit tests to see if everything still works after your code changes.

Normally, you want to execute all tests:

```
cd src
python -m unittest discover -s tests
```

You can also execute a single test suite only

```
python -m unittest tests.test_crawler
```
