import sys
import requests
import json
import os
import pickle
from urllib.parse import urlparse
import urllib.robotparser
import logging
from typing import Dict, Optional
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RobotsCache:
    def __init__(self, cache_file: str = "robots_cache.pkl", cache_duration: int = 86400):
        """Initialize robots.txt cache
        
        Args:
            cache_file: Path to cache file
            cache_duration: Cache validity in seconds (default 24 hours)
        """
        self.cache_file = cache_file
        self.cache_duration = timedelta(seconds=cache_duration)
        self.cache: Dict[str, tuple] = self._load_cache()
        
    def _load_cache(self) -> Dict:
        """Load cache from disk"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                logger.error(f"Error loading cache: {e}")
        return {}
    
    def _save_cache(self):
        """Save cache to disk"""
        try:
            with open(self.cache_file, 'wb') as f:
                pickle.dump(self.cache, f)
        except Exception as e:
            logger.error(f"Error saving cache: {e}")
    
    def get_robots_txt(self, domain: str) -> Optional[str]:
        """Get robots.txt content from cache if valid"""
        if domain in self.cache:
            content, timestamp = self.cache[domain]
            if datetime.now() - timestamp < self.cache_duration:
                return content
        return None
    
    def set_robots_txt(self, domain: str, content: str):
        """Store robots.txt content in cache"""
        self.cache[domain] = (content, datetime.now())
        self._save_cache()

class RobotsChecker:
    def __init__(self, cache_file: str = "robots_cache.pkl", enabled: bool = True):
        self.enabled = enabled
        self.cache = RobotsCache(cache_file) if enabled else None

    def fetch_robots_txt(self, robots_url: str) -> Optional[str]:
        """Fetch and validate robots.txt content"""
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
        except Exception as e:
            logger.error(f"Error fetching robots.txt: {str(e)}")
            return None

    def get_domain(self, url: str) -> str:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    
    def check_robots(self, url: str, user_agent: str = "*") -> Dict:
        if not self.enabled:
            return {"can_fetch": True, "error": None}
            
        try:
            domain = self.get_domain(url)
            robots_url = f"{domain}/robots.txt"
            
            # Try to get from cache first
            robots_txt = self.cache.get_robots_txt(domain) if self.cache else None
            
            # If not in cache, fetch and store
            if robots_txt is None:
                robots_txt = self.fetch_robots_txt(robots_url)
                if robots_txt and self.cache:
                    self.cache.set_robots_txt(domain, robots_txt)

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

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check if a URL is allowed to be crawled by a user agent.")
    parser.add_argument("url", help="The URL to check.")
    parser.add_argument("user_agent", nargs="?", default="*", help="The user agent to check for.")
    parser.add_argument("--disable-robots-check", action="store_true", help="Disable robots.txt checking.")
    
    args = parser.parse_args()
    
    checker = RobotsChecker(enabled=not args.disable_robots_check)
    try:
        result = checker.check_robots(args.url, args.user_agent)
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)