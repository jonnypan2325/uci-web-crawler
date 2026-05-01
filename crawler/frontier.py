import os
import shelve
import time

from threading import RLock, Condition
from urllib.parse import urlparse

from collections import deque
from utils import get_logger, get_urlhash, normalize
from scraper import is_valid, reset_analytics


class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = deque()
        self.lock = RLock()
        self.cond = Condition(self.lock)
        self.domain_in_use = set()
        self.domain_last_accessed = {}
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)

        if restart:
            reset_analytics()

        
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    @staticmethod
    def get_domain(url):
        return urlparse(url).netloc.lower()

    def get_tbd_url(self):
        # changed so that returns None if no url is available
        # looks for URL:
        #   1. who is not in any other thread,
        #   2. past the itnerval since last access to domain
        #   or else sleep for a bit and check again
        with self.lock:
            while True:
                now = time.monotonic()
                next_wait = None
                for url in self.to_be_downloaded:
                    domain = self.get_domain(url)
                    if domain in self.domain_in_use:
                        continue
                    last_accessed = self.domain_last_accessed.get(domain, 0)
                    remaining = self.config.time_delay - (now - last_accessed)
                    if remaining > 0:
                        if next_wait is None or remaining < next_wait:
                            next_wait = remaining
                        continue
                    self.to_be_downloaded.remove(url)
                    self.domain_in_use.add(domain)
                    self.domain_last_accessed[domain] = now
                    return url
                if not self.to_be_downloaded and not self.domain_in_use:
                    self.cond.notify_all()
                    return None
                # Either nothing is eligible yet (politeness) or the queue is
                # empty but other workers are still in flight. Wait until a
                # worker calls add_url / finish_domain, or the politeness
                # window for the soonest-eligible domain expires.
                self.cond.wait(timeout=next_wait)

    def finish_domain(self, url):
        domain = self.get_domain(url)
        with self.lock:
            self.domain_in_use.discard(domain)
            self.domain_last_accessed[domain] = time.monotonic()
            self.cond.notify_all()


    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        with self.lock:
            if urlhash not in self.save:
                self.save[urlhash] = (url, False)
                self.save.sync()
                self.to_be_downloaded.append(url)
                self.cond.notify()

    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        with self.lock:
            if urlhash not in self.save:
                # This should not happen.
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")

            self.save[urlhash] = (url, True)
            self.save.sync()

