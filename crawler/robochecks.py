import sys
import requests
import json
import os
import pickle
import argparse
from urllib.parse import urlparse
import urllib.robotparser
import logging
from typing import Dict, Optional, Tuple, Any, cast
from datetime import datetime, timedelta
from threading import Lock
from bs4 import BeautifulSoup

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
        self.cache: Dict[str, Tuple[str, datetime]] = self._load_cache()
        self.lock = Lock()
        
    def _load_cache(self) -> Dict[str, Tuple[str, datetime]]:
        """Load cache from disk"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'rb') as f:
                    return cast(Dict[str, Tuple[str, datetime]], pickle.load(f))
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
        with self.lock:
            if domain in self.cache:
                content, timestamp = self.cache[domain]
                if datetime.now() - timestamp < self.cache_duration:
                    return content
            return None
    
    def set_robots_txt(self, domain: str, content: str):
        with self.lock:
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
                # Strict validation: Must be text/plain.
                if 'text/plain' not in content_type:
                    logger.warning(f"Invalid content-type '{content_type}' for robots.txt at {robots_url}")
                    return None
                return resp.text
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching robots.txt: {str(e)}")
            return None

    def get_domain(self, url: str) -> str:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    
    def parse_meta_robots(self, html_content: str) -> Dict[str, Any]:
        """Parses the robots meta tag from the given HTML content."""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            meta_tag = soup.find('meta', attrs={'name': 'robots'})
            if meta_tag and meta_tag.get('content'):
                content = meta_tag.get('content', '').lower()
                return {
                    "can_index": "noindex" not in content,
                    "can_follow": "nofollow" not in content,
                    "error": None
                }
        except Exception as e:
            logger.error(f"Error parsing meta robots tag: {e}")
            return {"can_index": False, "can_follow": False, "error": str(e)}
        
        return {"can_index": True, "can_follow": True, "error": "No meta tag found"}
    
    def check_robots(self, url: str, user_agent: str = "*") -> Dict[str, Any]:
        if not self.enabled:
            return {"can_fetch": True, "error": None}
        
        robots_url: Optional[str] = None
        try:
            domain = self.get_domain(url)
            robots_url = f"{domain}/robots.txt"
            
            robots_txt = self.cache.get_robots_txt(domain) if self.cache else None
            
            if robots_txt is None:
                robots_txt = self.fetch_robots_txt(robots_url)
                if robots_txt and self.cache:
                    self.cache.set_robots_txt(domain, robots_txt)

            result: Dict[str, Any] = {
                "can_fetch": True, # Default to True (fail-open)
                "error": None,
            }

            if robots_txt is None:
                result["error"] = f"No valid robots.txt found at {robots_url}. Assuming allowed."
                return result

            try:
                rp = urllib.robotparser.RobotFileParser()
                rp.set_url(robots_url)
                rp.parse(robots_txt.splitlines())
                
                can_fetch = rp.can_fetch(user_agent, url)
                result["can_fetch"] = can_fetch
            except Exception as e:
                result["error"] = f"Error parsing robots.txt: {str(e)}"
                logger.error(result["error"])
                
            return result
        except Exception as e:
            return {
                "can_fetch": True, # Fail-open
                "error": f"Unexpected error in check_robots: {str(e)}",
            }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check if a URL is allowed to be crawled by a user agent.")
    parser.add_argument("url", nargs="?", default=None, help="The URL to check.")
    parser.add_argument("user_agent", nargs="?", default="*", help="The user agent to check for.")
    parser.add_argument("--disable-robots-check", action="store_true", help="Disable robots.txt checking.")
    
    args = parser.parse_args()

    if not args.url:
        parser.error("the following arguments are required: url")
    
    checker = RobotsChecker(enabled=not args.disable_robots_check)
    try:
        # The main CLI entrypoint now checks everything
        robots_result = checker.check_robots(args.url, args.user_agent)
        meta_result = checker.check_meta_robots(args.url)
        
        # Combine results
        final_result = {**robots_result, **meta_result}
        print(json.dumps(final_result, indent=2))

    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)