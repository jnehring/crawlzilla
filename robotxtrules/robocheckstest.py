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
        sys.executable, robochecks_path
    ]
    if args.force:
        robochecks_cmd.append("--disable-robots-check")
    
    robochecks_cmd.extend([args.url, args.user_agent])
    
    result = subprocess.run(robochecks_cmd, capture_output=True, text=True)
    try:
        robochecks_json = json.loads(result.stdout.strip())
    except Exception as e:
        print("Error parsing robochecks.py output:", result.stdout)
        sys.exit(1)

    print("Robochecks result:", json.dumps(robochecks_json, indent=2))

    if args.force:
        print("Ignoring robots.txt check and proceeding due to --force flag.")
    else:
        can_crawl = robochecks_json.get("can_fetch", False)
        if can_crawl:
            print("Crawling is allowed by robots.txt. Proceeding...")
        else:
            print("Crawling is disallowed by robots.txt. Use --force to override.")
            sys.exit(2)

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