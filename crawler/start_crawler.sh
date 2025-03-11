#!/usr/bin/env bash

cd "$(dirname "$0")"

export HF_HOME=/data/nehring/cache/hf_home
export PIP_CACHE_DIR=/data/nehring/cache/pip

pip3 install --cache-dir /data/nehring/cache/pip -r requirements.txt

python3 crawler.py \
    --output_folder ../outputs/kin/ \
    --round_size 5000 \
    --download_batch_size 1000 \
    --download_n_threads 30
