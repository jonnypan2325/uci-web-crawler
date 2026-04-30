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

    print("requested:", test_url)
    print("fetched:", resp.url)
    print("status:", resp.status)
    print("num links:", len(links))
    print(*links[:50], sep="\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())