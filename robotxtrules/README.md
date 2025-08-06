# Robots.txt Checker & Crawler Integration

This directory contains a tool to respectfully check a website's `robots.txt` rules before crawling. It is designed to be efficient, using an on-disk cache to avoid redundant downloads.

---

## Core Features

- **Respectful Crawling**: Uses Python's standard `urllib.robotparser` to parse `robots.txt` files and determine if a URL is allowed to be crawled.
- **Efficient Caching**: Automatically caches `robots.txt` files to disk (`robots_cache.pkl`). This prevents re-downloading the same file for every URL on a domain, making checks significantly faster. The cache expires after 24 hours by default.
- **Standalone & Integrated**: Can be run as a standalone script to check a single URL or integrated into a larger workflow using the `robocheckstest.py` harness.
- **Configurable**: The `robots.txt` check can be disabled, allowing a crawler to proceed regardless of the rules (e.g., for testing).

---

## How It Works

The system is composed of two main Python scripts:

- **`robochecks.py`**: The core logic for checking `robots.txt`.
    - **`RobotsChecker` class**: Fetches, parses, and evaluates `robots.txt` rules for a given URL.
    - **`RobotsCache` class**: Manages the on-disk cache. It loads `robots.txt` content from `robots_cache.pkl` if a valid, non-expired entry exists, or saves a newly fetched file to the cache.

- **`robocheckstest.py`**: An integration script that demonstrates how to use `robochecks.py` before running a crawler. It:
  1. Calls `robochecks.py` to check if crawling is permitted for a seed URL.
  2. If allowed, it launches the main crawler (`../crawler/crawler.py`).
  3. If disallowed, it exits gracefully without crawling.

---

## Caching Mechanism

To improve efficiency, the tool caches the content of `robots.txt` files in a file named **`robots_cache.pkl`** in the same directory.

- When you check a URL (e.g., `https://example.com/page1`), the script downloads `https://example.com/robots.txt` and stores it in the cache.
- When you check another URL on the same domain (e.g., `https://example.com/page2`), the script reads the cached `robots.txt` from the file instead of downloading it again.
- The cache is valid for **24 hours**. After that, the script will re-download the `robots.txt` file to ensure the rules are up-to-date.
- You can safely delete `robots_cache.pkl` at any time to clear the cache.

---

## Usage

### 1. Check a Single URL

To quickly check if a URL is allowed, run `robochecks.py` directly.

```sh
python robochecks.py <url> [user-agent]
```
- `<url>`: The full URL you want to check.
- `[user-agent]`: (Optional) The user-agent string to check against. Defaults to `*`.

**Example:**
```sh
python robochecks.py https://www.kigalitoday.com/
```

**Sample Output (JSON):**
```json
{
  "robots_url": "https://www.kigalitoday.com/robots.txt",
  "user_agent": "*",
  "can_fetch": true,
  "crawl_delay": 1,
  "error": null,
  "robots_txt_sample": "# robots.txt\n# @url: https://www.kigalitoday.com\n...",
  "is_valid_robotstxt": true
}
```

### 2. Check and Crawl

To run a crawl job that respects `robots.txt`, use the `robocheckstest.py` script.

```sh
python robocheckstest.py <url> --language <lang_code> [options]
```

**Required Arguments:**
- `<url>`: The seed URL to check and crawl.
- `--language <lang_code>`: Language code for the crawler (e.g., `kin_Latn`).

**Optional Arguments:**
- `--force`: Ignore `robots.txt` rules and crawl anyway.
- `--num_rounds <N>`: Number of crawl rounds.
- `--log_level <level>`: Log level for the crawler.
- `--user_agent <agent>`: User-agent for the `robots.txt` check.

**Example:**
```sh
python robocheckstest.py https://www.kigalitoday.com/ --language kin_Latn --num_rounds 3
```
This command will first check `robots.txt`. If allowed, it will launch the crawler. Otherwise, it will print a message and exit.

