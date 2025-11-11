import asyncio

from indexer import Indexer
from node import Node
from network import NodeInfo

from .helpers import get_free_port


def test_indexer_publish_file(tmp_path):
    async def scenario():
        port = get_free_port()
        node = Node("127.0.0.1", port)
        await node.start()
        try:
            indexer = Indexer(node)
            file = tmp_path / "doc.txt"
            file.write_text("content")
            key = await indexer.publish_file(str(file))
            values = await node.get(key)
            assert values is not None
            assert values[0].filename == "doc.txt"
        finally:
            await node.stop(graceful=False)

    asyncio.run(scenario())
