from node import Metadata, Storage


def make_meta(host: str, port: int, filename: str) -> Metadata:
    return Metadata(host=host, port=port, filename=filename, size=10, checksum="deadbeef")


def test_storage_put_get_deduplication():
    storage = Storage()
    key = 42
    meta = make_meta("127.0.0.1", 8000, "file.txt")
    storage.put(key, meta)
    # inserting duplicate should not multiply entries
    storage.put(key, meta)
    values = storage.get(key)
    assert values is not None
    assert len(values) == 1
    assert values[0].filename == "file.txt"


def test_storage_remove_host_and_cleanup():
    storage = Storage()
    key1, key2 = 11, 12
    storage.put(key1, make_meta("127.0.0.1", 8000, "file1.txt"))
    storage.put(key1, make_meta("127.0.0.1", 8001, "file2.txt"))
    storage.put(key2, make_meta("127.0.0.1", 8000, "file3.txt"))

    storage.remove_host("127.0.0.1", 8000)
    assert storage.get(key1) and len(storage.get(key1)) == 1
    assert storage.get(key1)[0].port == 8001
    assert storage.get(key2) is None


def test_storage_interval_queries():
    storage = Storage()
    storage.put(5, make_meta("127.0.0.1", 8000, "a"))
    storage.put(15, make_meta("127.0.0.1", 8000, "b"))
    storage.put(250, make_meta("127.0.0.1", 8000, "c"))

    within = storage.keys_in_range(0, 20, m=8)
    assert set(within.keys()) == {5, 15}

    taken = storage.take_keys_in_range(200, 10, m=8)
    assert set(taken.keys()) == {250, 5}
    assert 250 not in storage.all_items()
    assert 5 not in storage.all_items()
