"""
Manual harness to test `extract_next_links` on one URL.

Run:
  ./.venv/bin/python manual_test_extract.py "https://www.ics.uci.edu/"
"""

from configparser import ConfigParser
import sys
from types import SimpleNamespace

from utils.config import Config
import scraper
from bs4 import BeautifulSoup
import re
from collections import Counter

from scraper import STOP_WORDS


def _direct_fetch_response(url: str):
    """
    Fetch a URL directly (no Spacetime cache server) and adapt it to the
    minimal interface `extract_next_links` expects.
    """
    import requests

    # Avoid environment proxy variables interfering with a simple manual fetch.
    # (Some environments set HTTPS_PROXY/HTTP_PROXY that can block requests.)
    s = requests.Session()
    s.trust_env = False
    r = s.get(url, timeout=20)
    raw = SimpleNamespace(
        url=r.url,
        content=r.content,
        headers=r.headers,
    )
    return SimpleNamespace(url=url, status=r.status_code, error=None, raw_response=raw)


def main() -> int:
    test_url = sys.argv[1] if len(sys.argv) > 1 else "https://www.ics.uci.edu/"

    # Preferred path (matches crawler): use Spacetime cache server if reachable.
    # If it blocks/fails (common off-campus / VPN issues), fall back to direct fetch.

    resp = _direct_fetch_response(test_url)

    # Initialize analytics globals expected by scraper helpers.
    scraper.load_analytics()

    links = scraper.extract_next_links(test_url, resp)
    content_type = resp.raw_response.headers.get("Content-Type", "")
    content_size = len(resp.raw_response.content)
    print("Content type:", content_type)
    print("Content size:", content_size)
    try: 
        soup = BeautifulSoup(resp.raw_response.content, "lxml")
    except Exception as e:
        return list()
    
    # we can track only the alphabetic words, as the analytics doesn't care about numbers or special characters
    page_text_content = soup.get_text(separator=" ")
    page_text_content = page_text_content.replace("’", "'").replace("‘", "'") # normalize by replacing apostrophes with single quotes
        # tested and actually doesn't change word count for the dataset tested. Will leave for now.
        
    # regex to match words with apostrophes
    words = re.findall(r"\b[a-zA-Z]+(?:'[a-zA-Z]+)*\b", page_text_content)
    words_lowercase = [word.lower() for word in words if word.lower() not in STOP_WORDS]
    print("Number of words:", len(words_lowercase))
    if len(words_lowercase) <= 100:
        print("Words:", words_lowercase)
    else:
        print("Too many words to print")
    freq = Counter(words_lowercase)
    print("Frequency of words:", freq.most_common(10))

    print("requested:", test_url)
    print("fetched:", resp.url)
    print("status:", resp.status)
    print("num links:", len(links))
    print(*links[:50], sep="\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())