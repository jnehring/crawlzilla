#!/usr/bin/env bash

cd "$(dirname "$0")"

export HF_HOME=/data/nehring/cache/hf_home
export PIP_CACHE_DIR=/data/nehring/cache/pip
export NLTK_DATA=/data/nehring/cache/nltk

pip3 install --cache-dir /data/nehring/cache/pip -r requirements.txt

python generate_dataset.py