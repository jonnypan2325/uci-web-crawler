import pytest

import os
import sys

# Ensure the repo root (where `scraper.py` lives) is importable under pytest.
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from scraper import ALLOWED_DOMAIN_SUFFIXES, is_valid, extract_next_links, reset_analytics
import scraper


def test_allowed_exact_host():
    assert is_valid("https://cs.uci.edu/index.html") is True


def test_allowed_subdomain_under_suffix():
    # Ensures we accept hosts like www.cs.uci.edu
    assert is_valid("https://www.ics.uci.edu/index.html") is True
    assert is_valid("https://www.cs.uci.edu/index.html") is True
    assert is_valid("https://www.informatics.uci.edu/index.html") is True
    assert is_valid("http://www.stat.uci.edu/index.html") is True
    assert is_valid("https://weloveschool.ics.uci.edu/index.html") is True

def test_allowed_subdomain_under_suffix_without_period():
    # Ensures we accept hosts like www.cs.uci.edu
    assert is_valid("https://wwwcs.uci.edu/index.html") is False

def test_disallowed_host_rejected():
    assert is_valid("https://example.com/index.html") is False


def test_domain_suffix_check_is_case_insensitive():
    assert is_valid("https://CS.UCI.EDU/index.html") is True


def test_domain_suffix_allows_trailing_dot_in_hostname():
    assert is_valid("https://cs.uci.edu./index.html") is True


def test_domain_suffix_check_handles_ports():
    assert is_valid("https://www.cs.uci.edu:1234/index.html") is True


def test_allowed_domain_suffixes_fixture_sanity():
    # Makes sure the test assumptions match the scraper configuration.
    assert "cs.uci.edu" in ALLOWED_DOMAIN_SUFFIXES


def test_disallowed_file_extension_pdf():
    assert is_valid("https://cs.uci.edu/paper.pdf") is False


def test_disallowed_file_extension_case_insensitive():
    assert is_valid("https://cs.uci.edu/paper.PDF") is False


def test_is_valid_strips_url_fragment():
    assert is_valid("https://cs.uci.edu/page.html#section") is True


def test_disallowed_extension_still_rejected_when_fragment_present():
    assert is_valid("https://cs.uci.edu/paper.pdf#section") is False


def test_path_ending_with_date_yyyy_mm_dd_rejected():
    assert is_valid("https://cs.uci.edu/2024-03-15") is False
    assert is_valid("https://cs.uci.edu/2024-03-15/") is False
    assert is_valid("https://cs.uci.edu/news/2024-03-15") is False
    assert is_valid("https://cs.uci.edu/news/2024-03-15/") is False


def test_path_ending_with_date_yyyy_mm_rejected():
    assert is_valid("https://cs.uci.edu/2024-03") is False
    assert is_valid("https://cs.uci.edu/2024-03/") is False
    assert is_valid("https://cs.uci.edu/news/2024-03") is False
    assert is_valid("https://cs.uci.edu/news/2024-03/") is False


def test_non_string_input_rejected():
    assert is_valid(None) is False


def test_wordlist_txt_url_is_valid():
    assert is_valid("https://ics.uci.edu/~kay/wordlist.txt") is True


def test_plaintext_page_is_counted_in_analytics():
    class _Raw:
        def __init__(self, content, page_url, headers):
            self.content = content
            self.url = page_url
            self.headers = headers

    class _Resp:
        def __init__(self):
            self.status = 200
            self.raw_response = _Raw(
                b"content " * 60,
                "https://ics.uci.edu/~kay/wordlist.txt",
                {"Content-Type": "text/plain; charset=utf-8"},
            )

    reset_analytics()
    links = extract_next_links("https://ics.uci.edu/~kay/wordlist.txt", _Resp())
    assert "https://ics.uci.edu/~kay/wordlist.txt" in scraper.unique_pages
    assert scraper.longest_page[1] > 0
    assert isinstance(links, list)

