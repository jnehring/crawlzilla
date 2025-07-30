# Crawler that crawls only specific languages

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

**Start Crawler**




python3 crawler.py \
    --output_folder ../outputs/kin_Latn \
    --language kin_Latn \
    --seed_file ../seeds/seeds_kin_Latn.txt.gz 
