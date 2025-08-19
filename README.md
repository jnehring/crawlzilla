# Crawlzilla

Web crawler that crawls specific languages only.

1. [Overview](#overview)
    * [Application Workflow](#application-workflow)
    * [Output Folder Structure](#output-folder-structure)
2. [Installation](#installation)
3. [Starting the crawler](#starting-the-crawler)
    * [Explanation of parameters](#explanation-of-parameters)
    * [Standard configuration for a crawler](#standard-configuration-for-a-crawler)
    * [Debug crawl for a single page](#debug-crawl-for-a-single-page)

## Overview

### Application Workflow

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

## Installation

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

## Starting the crawler

### Explanation of parameters

```
$ python crawler.py -h
usage: Crawler [-h] [--seed_file SEED_FILE] [--seed_url SEED_URL] --language LANGUAGE
               [--start_fresh] [--output_folder OUTPUT_FOLDER] [--num_rounds NUM_ROUNDS]
               [--round_size ROUND_SIZE] [--download_batch_size DOWNLOAD_BATCH_SIZE]
               [--download_n_threads DOWNLOAD_N_THREADS] [--log_level {info,debug}]

Crawl African Languages

options:
  -h, --help            show this help message and exit
  --seed_file SEED_FILE
                        Seed file
  --seed_url SEED_URL   Start with a single seed url. This overwrites --seed_file. It is used for
                        debugging.
  --language LANGUAGE   Which language to use. This is the ISO_639-3 code for the language and the
                        ISO 15924 code for the script, e.g. kin_Latn for Kinyarwanda in Latin
                        script.
  --start_fresh         Set to True to remove all previously crawled data and start fresh.
  --output_folder OUTPUT_FOLDER
                        Where to store the output.
  --num_rounds NUM_ROUNDS
                        How many rounds to download and parse. Set to -1 run until there are no
                        more URLs.
  --round_size ROUND_SIZE
                        How many URLs to download per round.
  --download_batch_size DOWNLOAD_BATCH_SIZE
                        How many URLs to download per batch.
  --download_n_threads DOWNLOAD_N_THREADS
                        How many threads to parallel download data.
  --log_level {info,debug}
                        Adjust the logging level
```

### Standard configuration for a crawler

```
python3 crawler.py \
    --output_folder ../outputs/kin_Latn \
    --language kin_Latn \
    --seed_file ../seeds/seeds_kin_Latn.txt.gz \
    --delete_parsed \
    --delete_html
```

### Debug crawl for a single page

```
python3 crawler.py \
    --output_folder ../outputs/kin_Latn \
    --language kin_Latn \
    --seed_url https://www.kigalitoday.com/amakuru/amakuru-mu-rwanda/article/perezida-kagame-yakiriye-impapuro-za-ambasaderi-mushya-w-u-bushinwa-mu-rwanda \
    --log_level debug \
    --num_rounds 10
```
