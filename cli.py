# cli.py
import argparse
import asyncio
import hashlib
import os
import sys
import config
from node import Node, Metadata
from network import NodeInfo, RPCClient
from utils import hash_bytes_to_int

async def start_node(host: str, port: int, bootstrap: str | None):
    bootstrap_info = None
    if bootstrap:
        h, p = bootstrap.split(":")
        bootstrap_info = NodeInfo(0, h, int(p))  # id placeholder; actual lookup done later

    node = Node(host, port, bootstrap_info)
    await node.start()
    print(f"Node started: {node.info_str()}")
    try:
        while True:
            await asyncio.sleep(5)
            print(node.info_str())
    except KeyboardInterrupt:
        print("Shutting down node...")
        await node.stop()

def _hash_file(filepath: str) -> str:
    sha = hashlib.sha1()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()

def _compute_metadata(node_host: str, node_port: int, filepath: str) -> tuple[int, Metadata]:
    filename = os.path.basename(filepath)
    size = os.path.getsize(filepath)
    checksum = _hash_file(filepath)
    key = hash_bytes_to_int(f"{filename}:{size}:{checksum}".encode(), config.M)
    meta = Metadata(
        host=node_host,
        port=node_port,
        filename=filename,
        size=size,
        checksum=checksum,
    )
    return key, meta

async def publish_file(node_host: str, node_port: int, filepath: str):
    if not os.path.isfile(filepath):
        print(f"[-] File not found: {filepath}", file=sys.stderr)
        return
    key, meta = _compute_metadata(node_host, node_port, filepath)
    client = RPCClient()
    entry = NodeInfo(0, node_host, node_port)
    try:
        resp = await client.call(
            entry,
            {"type": "ROUTE_PUT", "payload": {"key": key, "meta": meta.to_dict()}},
            timeout=config.RPC_TIMEOUT,
        )
    except Exception as e:
        print(f"[-] Failed to publish via {node_host}:{node_port}: {e}", file=sys.stderr)
        return
    if not resp.get("ok"):
        print(f"[-] Remote node error: {resp.get('error')}", file=sys.stderr)
        return
    result = resp.get("result", {}) or {}
    primary = result.get("primary") or {}
    primary_host = primary.get("host", node_host)
    primary_port = primary.get("port", node_port)
    replicas = []
    for entry in result.get("replicas") or []:
        h = entry.get("host")
        p = entry.get("port")
        if h is not None and p is not None:
            replicas.append(f"{h}:{p}")
    replica_note = f" (replicas: {', '.join(replicas)})" if replicas else ""
    print(f"[+] Published {meta.filename}, key={key}, primary={primary_host}:{primary_port}{replica_note}")

async def fetch_file(node_host: str, node_port: int, key: int):
    client = RPCClient()
    entry = NodeInfo(0, node_host, node_port)
    try:
        resp = await client.call(
            entry,
            {"type": "ROUTE_GET", "payload": {"key": key}},
            timeout=config.RPC_TIMEOUT,
        )
    except Exception as e:
        print(f"[-] Failed to query {node_host}:{node_port}: {e}", file=sys.stderr)
        return
    if not resp.get("ok"):
        print(f"[-] Remote node error: {resp.get('error')}", file=sys.stderr)
        return
    result = resp.get("result", {}) or {}
    trace = result.get("trace") or []
    formatted_trace = []
    for entry in trace:
        host = entry.get("host")
        port = entry.get("port")
        node_id = entry.get("id")
        if host is None or port is None or node_id is None:
            continue
        formatted_trace.append(f"{host}:{port} (id={node_id})")
    if formatted_trace:
        hops = max(0, len(formatted_trace) - 1)
        print(f"[route] hops={hops}: " + " -> ".join(formatted_trace))

    values = result.get("values")
    if not values:
        print(f"[-] File key {key} not found")
        return
    print(f"[+] Found entries for key={key}:")
    for raw in values:
        try:
            meta = Metadata.from_dict(raw)
        except Exception:
            print(f"  [!] Malformed metadata: {raw}", file=sys.stderr)
            continue
        print(f"  {meta.filename} from {meta.host}:{meta.port} (size={meta.size})")

def main():
    parser = argparse.ArgumentParser(description="Chord DHT CLI")
    sub = parser.add_subparsers(dest="cmd")

    p1 = sub.add_parser("start", help="Start a node")
    p1.add_argument("--host", required=True)
    p1.add_argument("--port", type=int, required=True)
    p1.add_argument("--bootstrap", help="Bootstrap node host:port")

    p2 = sub.add_parser("put", help="Publish file metadata")
    p2.add_argument("--host", required=True)
    p2.add_argument("--port", type=int, required=True)
    p2.add_argument("--file", required=True)

    p3 = sub.add_parser("get", help="Lookup file by key")
    p3.add_argument("--host", required=True)
    p3.add_argument("--port", type=int, required=True)
    p3.add_argument("--key", type=int, required=True)

    args = parser.parse_args()
    if args.cmd == "start":
        asyncio.run(start_node(args.host, args.port, args.bootstrap))
    elif args.cmd == "put":
        asyncio.run(publish_file(args.host, args.port, args.file))
    elif args.cmd == "get":
        asyncio.run(fetch_file(args.host, args.port, args.key))
    else:
        parser.print_help()

if __name__ == "__main__":
    main()