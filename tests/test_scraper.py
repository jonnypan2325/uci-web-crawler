import pytest

import os
import sys

# Ensure the repo root (where `scraper.py` lives) is importable under pytest.
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from scraper import ALLOWED_DOMAIN_SUFFIXES, is_valid


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


def test_non_string_input_raises_typeerror():
    # urlparse(None) does not raise in this environment; `is_valid` should fail closed.
    assert is_valid(None) is False

