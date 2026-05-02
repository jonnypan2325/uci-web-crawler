"""
A module for implementing exact- and near-webpage similarity detection using techniques discussed in lectures for COMPSCI 121 (S26).
"""
from collections import deque
from collections.abc import Callable, Set
from itertools import islice
from math import floor, sin
from typing import Any, Iterable, Iterator, Literal


def is_exact_duplicate(candidate_text: str, *,
                       detection_method: Literal["additive_checksum", "md5"] = "md5") -> bool:
    """
    Checks if the candidate text is an exact duplicate of a previously-seen text according to a specified detection method.
    
    :param candidate_text: The candidate text to check for exact duplication.
    :param detection_method: The detection method used to check the candidate text for exact duplication.
    Must be one of 'additive_checksum' or 'md5'. Defaults to 'md5'.
    :return: True if the candidate text is an exact duplicate of a previously-seen text according to a specified detection method; False otherwise.
    """
    dispatch: dict[Literal["additive_checksum", "md5"], Callable[[str], bool]] = {
        "additive_checksum": has_duplicate_additive_checksum,
        "md5": has_duplicate_md5
    }
    try:
        return dispatch[detection_method](candidate_text)
    except KeyError as error:
        raise ValueError("Detection method must be one of 'additive_checksum' or 'md5'.") from error


def _preprocess_md5_message(message: str) -> bytearray:
    """
    Preprocesses the message passed to the MD5 hashing function.
    
    :param message: The message passed to the MD5 hashing function.
    :return: The preprocessed message.
    """
    message_bytearray = bytearray(message.encode("utf-8"))
    bit_length = len(message_bytearray) * 8
    # NOTE: Per MD5 pseudocode comments on Wikipedia
    # we can append 0x80 and pad with 0x00 bytes until the message length in bytes is
    # 56 (mod 64)
    message_bytearray.append(0x80)
    while len(message_bytearray) % 64 != 56:
        message_bytearray.append(0x00)
    message_bytearray.extend(bit_length.to_bytes(8, byteorder = "little"))
    return message_bytearray


WORD_MASK = 0XFFFFFFFF
def _left_rotate(x: int, amount: int) -> int:
    """
    Left rotates a 32-bit integer by the specified amount.
    
    :param x: The integer to left-rotate.
    :param amount: The amount to left-rotate the integer by.
    :return: The 32-bit integer left-rotated by the specified amount.
    """
    x &= WORD_MASK
    return ((x << amount) | (x >> (32 - amount))) & WORD_MASK


def compute_md5_hash(text: str) -> str:
    """
    Computes the MD5 hash of a text (according to https://en.wikipedia.org/wiki/MD5).
    
    :param text: The text to compute the MD5 hash of.
    :return: The MD5 hash of the text.
    """
    # NOTE: All values are little-endian, per the MD5 pseudocode comments
    shift_groups = (
        [7, 12, 17, 22],
        [5, 9, 14, 20],
        [4, 11, 16, 23],
        [6, 10, 15, 21]
    )
    s = []
    for shifts in shift_groups:
        s.extend(shifts * 4)
    K: list[int] = []
    for i in range(64):
        K.append(floor(2 ** 32 * abs(sin(i + 1))))
    a0 = 0x67452301
    b0 = 0xEFCDAB89
    c0 = 0x98BADCFE
    d0 = 0x10325476
    message = _preprocess_md5_message(text)
    # Process the message in successive 64-byte (512-bit) chunks
    for chunk_starting_index in range(0, len(message), 64):
        chunk = message[chunk_starting_index:chunk_starting_index + 64]
        # Break chunk into 16 4-byte (32-bit) words
        # Because we add elements of M to F, we must convert each 4 byte bytearray into an int
        M: list[int] = [int.from_bytes(chunk[4 * j:4 * j + 4], byteorder = "little") for j in range(16)]
        A = a0
        B = b0
        C = c0
        D = d0
        for i in range(64):
            if 0 <= i <= 15:
                F = (B & C) | ((~B) & D)
                g = i
            elif 16 <= i <= 31:
                F = (D & B) | ((~D) & C)
                g = (5 * i + 1) % 16
            elif 32 <= i <= 47:
                F = (B ^ C ^ D)
                g = (3 * i + 5) % 16
            else:
                F = C ^ (B | (~D))
                g = (7 * i) % 16
            # Apply WORD_MASK to keep F as a 32-bit integer
            F = (F + A + K[i] + M[g]) & WORD_MASK
            A = D
            D = C
            C = B
            # Apply WORD_MASK to keep B as a 32-bit integer
            B = (B + _left_rotate(F, s[i])) & WORD_MASK
        # Apply WORD_MASK to keep a0, b0, c0, and d0 as 32-bit integers
        a0 = (a0 + A) & WORD_MASK
        b0 = (b0 + B) & WORD_MASK
        c0 = (c0 + C) & WORD_MASK
        d0 = (d0 + D) & WORD_MASK
    # Digest is a0 append b0 append c0 append d0 (little-endian)
    digest = b"".join(
        word.to_bytes(4, byteorder = "little")
        for word in (a0, b0, c0, d0)
    )
    # Output in hexadecimal for readability
    return digest.hex()


SEEN_MD5_HASH: set[str] = set()
def has_duplicate_md5(candidate_text: str) -> bool:
    """
    Checks if the candidate text is an exact duplicate of a previously-seen text using the MD5 hashing function.
    
    :param candidate_text: The candidate text to check for exact duplication (via the MD5 hashing function).
    :return: True if the candidate text is an exact duplicate of a previously-seen text according to its MD5 hash.
    """
    md5_hash = compute_md5_hash(candidate_text)
    if md5_hash in SEEN_MD5_HASH:
        return True
    SEEN_MD5_HASH.add(md5_hash)
    return False


def compute_additive_checksum(text: str) -> int:
    """
    Computes the additive checksum of a text. The additive checksum of a text is the sum of the bytes in the text.
    
    :param text: The text to compute the additive checksum of.
    :return: The additive checksum of the text.
    """
    return sum(text.encode("utf-8"))


SEEN_ADDITIVE_CHECKSUM: set[int] = set()
def has_duplicate_additive_checksum(candidate_text: str) -> bool:
    """
    Checks if the candidate text is an exact duplicate of a previously-seen text using an additive checksum.
    The additive checksum of a text is the sum of the bytes in the text.
    
    :param candidate_text: The candidate text to check for exact duplication (via an additive checksum).
    :return: True if the candidate text is an exact duplicate of a previously-seen text according to its additive checksum; False otherwise.
    """
    additive_checksum = compute_additive_checksum(candidate_text)
    if additive_checksum in SEEN_ADDITIVE_CHECKSUM:
        return True
    SEEN_ADDITIVE_CHECKSUM.add(additive_checksum)
    return False


def is_near_duplicate(candidate_text: str, *,
                      detection_method: Literal["fingerprint"] = "fingerprint",
                      similarity_threshold: float,
                      **detector_kwargs: Any) -> bool:
    """
    Checks if the candidate text is a near-duplicate of a previously-seen text according to a specified detection method.
    
    :param candidate_text: The candidate text to check for near-duplication.
    :param detection_method: The detection method used to check the candidate text for near-duplication. Must be 'fingerprint'.
    :param similarity_threshold: The minimum Jaccard similarity (inclusive) for the candidate text to be considered a near-duplicate of a previously-seen text.
    :return: True if the candidate text is a near-duplicate of a previously-seen text according to a specified detection method; False otherwise.
    """
    # TODO: Implement simhashing.
    dispatch: dict[Literal["fingerprint"], Callable[..., bool]] = {
        "fingerprint": has_near_duplicate_fingerprint,
    }
    try:
        detector = dispatch[detection_method]
    except KeyError as error:
        raise ValueError("Detection method must be 'fingerprint'.") from error
    return detector(candidate_text,
                    similarity_threshold = similarity_threshold,
                    **detector_kwargs)


def compute_n_grams(tokens: Iterable[str], *, n: int) -> Iterator[tuple[str, ...]]:
    """
    Computes all n-grams obtained from a sliding window over an iterable of tokens.
    
    :param tokens: The iterable of tokens used to compute all n-grams from.
    :param n: The number of tokens in an n-gram.
    :return: An iterator of all n-grams obtained from a sliding window over an iterable of tokens.
    """
    if n <= 0:
        raise ValueError("The number of tokens in an n-gram must be positive.")
    tokens_iterator = iter(tokens)
    sliding_window = deque(islice(tokens_iterator, n), maxlen = n)
    if len(sliding_window) < n:
        return
    yield tuple(sliding_window)
    for next_token in tokens_iterator:
        sliding_window.append(next_token)
        yield tuple(sliding_window)


def winnowing(hashes: Iterable[str], *, sliding_window_size: int) -> tuple[str, ...]:
    """
    Executes the winnowing algorithm on the iterable of hashes to select a representative collection of hashes.
    
    :param hashes: The iterable of hashes to execute the winnowing algorithm on.
    :param sliding_window_size: The size of the sliding window used in the winnowing algorithm.
    :return: A tuple of representative hashes.
    """
    if sliding_window_size <= 0:
        raise ValueError("The sliding window size must be positive.")
    hashes_iterator = iter(hashes)
    sliding_window = deque(islice(hashes_iterator, sliding_window_size), maxlen = sliding_window_size)
    if len(sliding_window) == 0:
        return ()
    if len(sliding_window) < sliding_window_size:
        return (min(sliding_window),)
    representative_hashes = [min(sliding_window)]
    for next_hash in hashes_iterator:
        sliding_window.append(next_hash)
        representative_hashes.append(min(sliding_window))
    return tuple(representative_hashes)


def compute_fingerprint(text: str, *, n: int, sliding_window_size: int) -> tuple[str, ...]:
    """
    Computes the fingerprint of a text.
    
    :param text: The text to compute the fingerprint of.
    :param n: The number of tokens in an n-gram (for computing the fingerprint).
    :param sliding_window_size: The size of the sliding window used in the winnowing algorithm (for selecting representative hashes).
    :return: The fingerprint of the text.
    """
    tokens = text.split()
    n_grams = compute_n_grams(tokens, n = n)
    hashes = (compute_md5_hash(" ".join(n_gram)) for n_gram in n_grams)
    return winnowing(hashes, sliding_window_size = sliding_window_size)


def jaccard_similarity(a: Set, b: Set) -> float:
    """
    Computes the Jaccard similarity of two sets.
    
    :param a: The first set.
    :param b: The second set.
    :return: The Jaccard similarity of the two sets.
    """
    union = a | b
    # NOTE: The Jaccard similarity is undefined when both sets are empty.
    # If both sets are empty, then define their Jaccard similarity to be 0.0.
    if not union:
        return 0.0
    return len(a & b) / len(union)


SEEN_FINGERPRINTS: set[frozenset[str]] = set()
def has_near_duplicate_fingerprint(candidate_text: str, *,
                                   similarity_threshold: float,
                                   n: int, sliding_window_size: int) -> bool:
    """
    Checks if the candidate text is a near-duplicate of a previously-seen text using fingerprints.
    
    :param candidate_text: The candidate text to check for near-duplication (via fingerprints).
    :param similarity_threshold: The minimum Jaccard similarity (inclusive) for the candidate text to be considered a near-duplicate of a previously-seen text.
    :param n: The number of tokens in an n-gram (for computing the fingerprint).
    :param sliding_window_size: The size of the sliding window used in the winnowing algorithm (for computing the fingerprint).
    :return: True if the candidate text is a near-duplicate of a previously-seen text according to its fingerprint; False otherwise.
    """
    if not 0 <= similarity_threshold <= 1:
        raise ValueError("The similarity threshold must be between 0 and 1.")
    fingerprint = compute_fingerprint(candidate_text,
                                      n = n,
                                      sliding_window_size = sliding_window_size)
    fingerprint_frozenset = frozenset(fingerprint)
    # NOTE: The candidate text has an empty fingerprint when the candidate text has fewer than n tokens
    if not fingerprint_frozenset:
        # NOTE: If the candidate text has an empty fingerprint, near-duplicate detection is unreliable
        return False
    for seen_fingerprint in SEEN_FINGERPRINTS:
        if jaccard_similarity(fingerprint_frozenset, seen_fingerprint) >= similarity_threshold:
            return True
    SEEN_FINGERPRINTS.add(fingerprint_frozenset)
    return False
