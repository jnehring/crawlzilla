import sys
import argparse
import subprocess
import json
import os

def main():
    parser = argparse.ArgumentParser(description="Test robots.txt and crawl if allowed.")
    parser.add_argument("url", help="URL to check and crawl")
    parser.add_argument("--language", required=True, help="Language code for the crawler (e.g. kin_Latn)")
    parser.add_argument("--retries", type=int, default=1, help="Number of times to retry fetching robots.txt")
    parser.add_argument("--force", action="store_true", help="Ignore robots.txt rules and crawl anyway")
    parser.add_argument("--num_rounds", type=int, default=1, help="Number of crawl rounds")
    parser.add_argument("--log_level", default="info", help="Log level for the crawler")
    parser.add_argument("--output_folder", default="./robochecks_output", help="Output folder for crawl results")
    parser.add_argument("--user_agent", default="*", help="User-agent to check in robots.txt")
    args = parser.parse_args()

    # Call robochecks.py and get JSON result
    robochecks_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "robochecks.py"))
    robochecks_cmd = [
        sys.executable, robochecks_path, args.url, args.user_agent
    ]
    result = subprocess.run(robochecks_cmd, capture_output=True, text=True)
    try:
        robochecks_json = json.loads(result.stdout.strip())
    except Exception as e:
        print("Error parsing robochecks.py output:", result.stdout)
        sys.exit(1)

    print("Robochecks result:", json.dumps(robochecks_json, indent=2))

    can_crawl = robochecks_json.get("can_fetch", False)
    if not can_crawl and not args.force:
        print("Crawling is disallowed by robots.txt. Use --force to override.")
        sys.exit(2)
    else:
        if not can_crawl:
            print("Proceeding to crawl anyway due to --force flag.")
        else:
            print("Crawling is allowed by robots.txt. Proceeding...")

    # Prepare output folder (in current directory)
    output_folder = os.path.abspath(args.output_folder)
    os.makedirs(output_folder, exist_ok=True)

    # Build the crawler command (using your existing crawler.py)
    crawler_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../crawler/crawler.py"))
    cmd = [
        sys.executable, crawler_path,
        "--output_folder", output_folder,
        "--language", args.language,
        "--seed_url", args.url,
        "--log_level", args.log_level,
        "--num_rounds", str(args.num_rounds)
    ]
    print("\nRunning crawler with command:")
    print(" ".join(cmd))
    subprocess.run(cmd)

if __name__ == "__main__":
    main()



""" import sys
import requests
import json
from urllib.parse import urlparse
import urllib.robotparser
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_robots_url(url):
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}/robots.txt"

def fetch_robots_txt(robots_url):
    try:
        resp = requests.get(robots_url, timeout=10)
        if resp.status_code == 200:
            content_type = resp.headers.get('content-type', '').lower()
            content = resp.text.lower()
            
            # validation
            is_valid = (
                ('text/plain' in content_type and not content.startswith('<!doctype')) or
                any(directive in content for directive in [
                    'user-agent:', 'disallow:', 'allow:', 'sitemap:'
                ])
            )
            
            if is_valid:
                return resp.text
            logger.warning(f"Invalid robots.txt content received from {robots_url}")
            return None
        else:
            logger.warning(f"Failed to fetch robots.txt: HTTP {resp.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error fetching robots.txt: {str(e)}")
        return None

def check_robots(url, user_agent="*"):
    try:
        robots_url = get_robots_url(url)
        robots_txt = fetch_robots_txt(robots_url)
        result = {
            "robots_url": robots_url,
            "user_agent": user_agent,
            "can_fetch": False,
            "crawl_delay": None,
            "error": None,
            "robots_txt_sample": None,
            "is_valid_robotstxt": False
        }
        
        if robots_txt is None:
            result["error"] = "No valid robots.txt found at the URL"
            return result
            
        try:
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(robots_url)
            rp.parse(robots_txt.splitlines())
            
            result["is_valid_robotstxt"] = True
            result["can_fetch"] = rp.can_fetch(user_agent, url)
            result["crawl_delay"] = rp.crawl_delay(user_agent)
            result["robots_txt_sample"] = "\n".join(robots_txt.splitlines()[:20])
            result["error"] = None
        except Exception as e:
            result["error"] = f"Error parsing robots.txt: {str(e)}"
            logger.error(result["error"])
            
        return result
    except Exception as e:
        return {
            "robots_url": robots_url if 'robots_url' in locals() else None,
            "user_agent": user_agent,
            "can_fetch": False,
            "crawl_delay": None,
            "error": f"Unexpected error: {str(e)}",
            "robots_txt_sample": None,
            "is_valid_robotstxt": False
        }

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python robochecks.py <url> [user_agent]")
        sys.exit(1)
        
    url = sys.argv[1]
    user_agent = sys.argv[2] if len(sys.argv) > 2 else "*"
    
    try:
        result = check_robots(url, user_agent)
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1) """
