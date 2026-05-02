"""
A module for testing the compute_md5_hash function implemented in similarity_detection.py.

Execute from project root with `python3 -m tests.test_compute_md5_hash`.
"""
from hashlib import md5
import random
import unittest
import sys

from similarity_detection import compute_md5_hash


class TestComputeMD5Hash(unittest.TestCase):
    def test_empty_string(self):
        self.assertEqual(md5("".encode("utf-8")).hexdigest(), compute_md5_hash(""))

    def test_random_strings(self):
        SEED = 0
        RANDOM_STRING_LENGTHS = [1, 10, 25, 50, 100]
        VALID_CHARS = [chr(i) for i in range(sys.maxunicode + 1) if not (0xD800 <= i <= 0xDFFF)]

        random.seed(SEED)

        for length in RANDOM_STRING_LENGTHS:
            for _ in range(5000):
                test_string = "".join(random.choices(VALID_CHARS, k = length))
                self.assertEqual(md5(test_string.encode("utf-8")).hexdigest(), compute_md5_hash(test_string))


if __name__ == "__main__":
    unittest.main()
