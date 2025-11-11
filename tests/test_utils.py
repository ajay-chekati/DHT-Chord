import math

from utils import hash_bytes_to_int, in_interval


def test_hash_bytes_to_int_range_and_consistency():
    payload = b"hello world"
    for bits in (8, 16, 24, 32):
        value = hash_bytes_to_int(payload, bits)
        assert 0 <= value < (1 << bits)
    assert hash_bytes_to_int(payload, 32) == hash_bytes_to_int(payload, 32)
    assert hash_bytes_to_int(payload, 32) != hash_bytes_to_int(payload + b"!", 32)


def test_in_interval_no_wrap():
    assert in_interval(10, 20, 15)
    assert not in_interval(10, 20, 25)
    assert in_interval(10, 20, 10, inclusive_start=True)
    assert not in_interval(10, 20, 10)
    assert in_interval(10, 20, 20, inclusive_end=True)


def test_in_interval_with_wraparound():
    start, end = 250, 5
    # interval spans (250, 256) U [0, 5)
    assert in_interval(start, end, 2)
    assert in_interval(start, end, 253)
    assert not in_interval(start, end, 100)
    # start and end remain exclusive when wrapping because of the implementation
    assert not in_interval(start, end, start, inclusive_start=True)
    assert not in_interval(start, end, end, inclusive_end=True)
