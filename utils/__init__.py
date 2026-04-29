import os
import logging
from hashlib import sha256
from urllib.parse import urlparse, urldefrag, urlunparse

def get_logger(name, filename=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not os.path.exists("Logs"):
        os.makedirs("Logs")
    fh = logging.FileHandler(f"Logs/{filename if filename else name}.log")
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
       "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def get_urlhash(url):
    parsed = urlparse(url)
    # everything other than scheme.
    return sha256(
        f"{parsed.netloc}/{parsed.path}/{parsed.params}/"
        f"{parsed.query}/{parsed.fragment}".encode("utf-8")).hexdigest()

def normalize(url):

    # strip fragment so in page links are not treated as different urls.
    url, fragment = urldefrag(url)

    # lowercase the scheme and hostname
    parsed = urlparse(url)
    scheme_lower = parsed.scheme.lower()
    netloc_lower = parsed.netloc.lower()
    parsed = parsed._replace(scheme=scheme_lower, netloc=netloc_lower)

    url = urlunparse(parsed)
    if url.endswith("/"):
        return url.rstrip("/")
    return url
