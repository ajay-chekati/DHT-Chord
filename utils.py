# utils.py
import hashlib
from typing import Optional

def hash_bytes_to_int(data: bytes, m: int = 32) -> int:
    """
    Hash bytes into an integer in range [0, 2^m).
    Default m=32 for easier local testing. Use config.M for real runs.
    """
    h = hashlib.sha1(data).digest()
    # take leading m bits from sha1
    total_bits = len(h) * 8
    if m >= total_bits:
        return int.from_bytes(h, "big")
    # convert to int then truncate to m bits
    v = int.from_bytes(h, "big")
    return v >> (total_bits - m)

def in_interval(start: int, end: int, x: int, inclusive_start: bool=False, inclusive_end: bool=False, m: int = 32) -> bool:
    """
    Return whether x ∈ (start, end) on ring modulo 2^m.
    inclusive_start/end control closed endpoints.
    Handles wrap-around.
    """
    mod = 1 << m
    start %= mod
    end %= mod
    x %= mod

    if start < end:
        left = x > start or (inclusive_start and x == start)
        right = x < end or (inclusive_end and x == end)
        return left and right
    elif start > end:
        # wrap-around: (start, mod) U [0, end)
        if x > start or x < end:
            if x == start:
                return inclusive_start
            if x == end:
                return inclusive_end
            return True
        return False
    else:
        # start == end means full circle if inclusive, else empty
        if inclusive_start or inclusive_end:
            return True
        return False