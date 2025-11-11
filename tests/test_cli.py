import os
from pathlib import Path

import config
from cli import _compute_metadata


def test_compute_metadata(tmp_path: Path):
    file = tmp_path / "sample.txt"
    file.write_text("hello world")

    key, meta = _compute_metadata("127.0.0.1", 9000, str(file))

    assert meta.filename == "sample.txt"
    assert meta.host == "127.0.0.1"
    assert meta.port == 9000
    assert meta.size == os.path.getsize(file)
    assert len(meta.checksum) == 40
    assert 0 <= key < (1 << config.M)
