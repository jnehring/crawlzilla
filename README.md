# Crawler that crawls only specific languages

## Overview

### Architecture

<img src="/images/flowchart.drawio.png" />

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

````
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

Standard configuration for a crawler

```
python3 crawler.py \
    --output_folder ../outputs/kin_Latn \
    --language kin_Latn \
    --seed_file ../seeds/seeds_kin_Latn.txt.gz \
    --delete_parsed \
    --delete_html
```

This starts the crawler for a single page and with debug outputs. This is useful for debugging

```
python3 crawler.py \
    --output_folder ../outputs/kin_Latn \
    --language kin_Latn \
    --seed_url https://www.kigalitoday.com/amakuru/amakuru-mu-rwanda/article/perezida-kagame-yakiriye-impapuro-za-ambasaderi-mushya-w-u-bushinwa-mu-rwanda \
    --log_level debug \
    --num_rounds 10 \
    --delete_parsed \
    --delete_html
```
