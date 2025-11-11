# indexer.py
import os
import hashlib
from node import Node, Metadata
from utils import hash_bytes_to_int

class Indexer:
    def __init__(self, node: Node):
        self.node = node

    def _hash_file(self, filepath: str) -> str:
        """Return SHA1 checksum of file content."""
        sha = hashlib.sha1()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha.update(chunk)
        return sha.hexdigest()

    async def publish_file(self, filepath: str) -> int:
        """
        Compute metadata and store in DHT.
        Key = hash(filename + size + checksum)
        """
        filename = os.path.basename(filepath)
        size = os.path.getsize(filepath)
        checksum = self._hash_file(filepath)
        key = hash_bytes_to_int(f"{filename}:{size}:{checksum}".encode())

        meta = Metadata(
            host=self.node.host,
            port=self.node.port,
            filename=filename,
            size=size,
            checksum=checksum
        )
        await self.node.put(key, meta)
        print(f"[+] Published {filename}, key={key}")
        return key