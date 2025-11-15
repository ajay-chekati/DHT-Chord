# publish_folder.py
import asyncio, os, sys
from indexer import Indexer
from node import Node
from network import NodeInfo

async def publish(contact_host, contact_port, folder="../files"):
    # start a temporary node on ephemeral port (use contact as bootstrap)
    bootstrap = NodeInfo(0, contact_host, contact_port)
    # choose ephemeral port: OS pick by using port 0 with RPCServer is not available in CLI;
    # so pick a high port unlikely to clash
    temp_port = contact_port + 1000
    node = Node("127.0.0.1", temp_port, bootstrap=bootstrap)
    await node.start()
    idx = Indexer(node)
    for fname in sorted(os.listdir(folder)):
        fpath = os.path.join(folder, fname)
        if os.path.isfile(fpath):
            key = await idx.publish_file(fpath)
            print("Published", fname, "key=", key)
    await asyncio.sleep(1)
    await node.stop()

if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 6000
    folder = sys.argv[3] if len(sys.argv) > 3 else "files"
    asyncio.run(publish(host, port, folder))