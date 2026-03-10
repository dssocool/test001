"""
Deterministic hashing of the domain data generation key to choose masking rounds.
Do not use built-in hash() — it is randomized per process in Python 3.3+.

Rounds mapping (m = stable_hash % 4):
  m == 0 -> 4 rounds
  m == 1 -> 1 round
  m == 2 -> 2 rounds
  m == 3 -> 3 rounds

Empty key -> 1 round (matches previous single-run behavior).
"""
import zlib


def stable_hash_int(key):
    """
    Return a non-negative stable integer from a Unicode string.
    Uses CRC32 over UTF-8 bytes (stable across runs and processes).
    """
    if key is None:
        key = ""
    if not isinstance(key, str):
        key = str(key)
    raw = key.encode("utf-8")
    return zlib.crc32(raw) & 0xFFFFFFFF


def masking_rounds_from_key(key):
    """
    Return number of masking job rounds (1–4) for dry run.
    Empty or whitespace-only key -> 1 round.
    """
    if key is None or (isinstance(key, str) and not key.strip()):
        return 1
    m = stable_hash_int(key) % 4
    if m == 0:
        return 4
    return m
