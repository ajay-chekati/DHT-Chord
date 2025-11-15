# dump_store.py
import asyncio, sys
from network import NodeInfo, RPCClient

async def dump(port):
    client = RPCClient()
    node = NodeInfo(0, "127.0.0.1", port)
    try:
        resp = await client.call(node, {"type":"DUMP_STORE", "payload": {}}, timeout=3.0)
        print(resp)
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python dump_store.py <port>")
    else:
        asyncio.run(dump(int(sys.argv[1])))