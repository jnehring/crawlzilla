#!/usr/bin/env bash

cd "$(dirname "$0")"

export HF_HOME=/data/nehring/cache/hf_home
export PIP_CACHE_DIR=/data/nehring/cache/pip
export NLTK_DATA=/data/nehring/cache/nltk

python3 finalize_data.py