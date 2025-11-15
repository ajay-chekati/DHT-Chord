# scripts/probe_ring.py
import asyncio
from network import NodeInfo, RPCClient
from utils import hash_bytes_to_int
import config, sys

async def probe(ports):
    client = RPCClient()
    for p in ports:
        node = NodeInfo(0, "127.0.0.1", p)
        try:
            pong = await client.call(node, {"type":"PING","payload":{}}, timeout=2.0)
            nid = pong.get("result", {}).get("node", {}).get("id")
        except Exception as e:
            print(f"{p}: UNREACHABLE ({e})")
            continue

        try:
            resp = await client.call(node, {"type":"GET_PREDECESSOR","payload":{}}, timeout=2.0)
            pred = resp.get("result", {}).get("node")
        except Exception:
            pred = None

        try:
            resp2 = await client.call(node, {"type":"FIND_SUCCESSOR","payload":{"id": (int(nid) + 1) % (1 << config.M)}}, timeout=2.0)
            succ_responsible = resp2.get("result", {}).get("node")
        except Exception:
            succ_responsible = None

        try:
            resp3 = await client.call(node, {"type":"GET_SUCCESSOR_LIST","payload":{}}, timeout=2.0)
            chain = resp3.get("result", {}).get("nodes", [])
            succ_ptr = chain[0] if chain else None
        except Exception:
            succ_ptr = None

        print(f"Node(port={p}) id={nid} succ_ptr={succ_ptr} succ_responsible={succ_responsible} pred={pred}")

if __name__ == "__main__":
    # ports as command-line args or default 6000..6003
    args = sys.argv[1:]
    if args:
        ports = [int(x) for x in args]
    else:
        ports = [6000,6001,6002,6003]
    asyncio.run(probe(ports))