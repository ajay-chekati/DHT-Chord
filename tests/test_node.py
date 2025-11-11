import asyncio
import hashlib

import config
from network import NodeInfo
from node import Metadata, Node
from utils import hash_bytes_to_int

from .helpers import get_free_port


def test_node_single_startup_sets_successor():
    async def scenario():
        port = get_free_port()
        node = Node("127.0.0.1", port)
        await node.start()
        try:
            assert node.successor == node.info
            assert node.predecessor is None
            assert node.succ_list.primary() == node.info
        finally:
            await node.stop(graceful=False)

    asyncio.run(scenario())


def test_node_put_and_get_across_two_nodes():
    async def scenario():
        port_a = get_free_port()
        node_a = Node("127.0.0.1", port_a)
        await node_a.start()
        try:
            port_b = get_free_port()
            bootstrap = NodeInfo(0, "127.0.0.1", port_a)
            node_b = Node("127.0.0.1", port_b, bootstrap=bootstrap)
            await node_b.start()
            try:
                await asyncio.sleep(0.5)
                payload = b"test-payload"
                key = hash_bytes_to_int(payload, config.M)
                checksum = hashlib.sha1(payload).hexdigest()
                meta = Metadata(
                    host="127.0.0.1",
                    port=9999,
                    filename="sample.bin",
                    size=len(payload),
                    checksum=checksum,
                )

                await node_a.put(key, meta)
                await asyncio.sleep(0.5)

                values = await node_b.get(key)
                assert values is not None
                filenames = {v.filename for v in values}
                assert "sample.bin" in filenames
            finally:
                await node_b.stop(graceful=False)
        finally:
            await node_a.stop(graceful=False)

    asyncio.run(scenario())
