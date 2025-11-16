# publish_folder.py
import asyncio
import hashlib
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPTS_DIR.parent

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import config
from network import NodeInfo, RPCClient
from node import Metadata
from utils import hash_bytes_to_int
FILES_DIR = SCRIPTS_DIR / "files"
DEFAULT_OUTPUT = SCRIPTS_DIR / "published_keys.txt"


def _resolve_folder(folder: str | None) -> Path:
    if not folder:
        path = FILES_DIR
    else:
        candidate = Path(folder)
        path = candidate if candidate.is_absolute() else SCRIPTS_DIR / candidate
    if not path.is_dir():
        raise FileNotFoundError(f"Metadata folder not found: {path}")
    return path


def _resolve_output(path: str | None) -> Path:
    if not path:
        resolved = DEFAULT_OUTPUT
    else:
        candidate = Path(path)
        resolved = candidate if candidate.is_absolute() else SCRIPTS_DIR / candidate
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved


def _hash_file(filepath: Path) -> str:
    sha = hashlib.sha1()
    with filepath.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()


def _compute_metadata(host: str, port: int, filepath: Path) -> tuple[int, Metadata]:
    filename = filepath.name
    size = filepath.stat().st_size
    checksum = _hash_file(filepath)
    key = hash_bytes_to_int(f"{filename}:{size}:{checksum}".encode(), config.M)
    meta = Metadata(
        host=host,
        port=port,
        filename=filename,
        size=size,
        checksum=checksum,
    )
    return key, meta


async def publish_via_existing_node(host: str, port: int, folder: str | None = None) -> list[tuple[str, int, str, int]]:
    folder_path = _resolve_folder(folder)
    client = RPCClient()
    entry = NodeInfo(0, host, port)
    published: list[tuple[str, int, str, int]] = []

    for filepath in sorted(folder_path.iterdir()):
        if not filepath.is_file():
            continue
        key, meta = _compute_metadata(host, port, filepath)
        try:
            resp = await client.call(
                entry,
                {"type": "ROUTE_PUT", "payload": {"key": key, "meta": meta.to_dict()}},
                timeout=config.RPC_TIMEOUT,
            )
        except Exception as exc:
            print(f"[-] Failed to publish {filepath.name} via {host}:{port}: {exc}")
            continue
        if not resp.get("ok"):
            print(f"[-] Remote node error publishing {filepath.name}: {resp.get('error')}")
            continue

        result = resp.get("result", {}) or {}
        primary = result.get("primary") or {}
        primary_host = primary.get("host", host)
        primary_port = primary.get("port", port)

        replicas = []
        for entry_replica in result.get("replicas") or []:
            h = entry_replica.get("host")
            p = entry_replica.get("port")
            if h is not None and p is not None:
                replicas.append(f"{h}:{p}")

        replica_note = f" (replicas: {', '.join(replicas)})" if replicas else ""
        print(f"[+] Published {filepath.name}, key={key}, primary={primary_host}:{primary_port}{replica_note}")
        published.append((filepath.name, key, primary_host, primary_port))

    return published


def _write_output(entries: list[tuple[str, int, str, int]], path: str | None) -> Path:
    output_path = _resolve_output(path)
    with output_path.open("w", encoding="utf-8") as out:
        for filename, key, primary_host, primary_port in entries:
            out.write(f"{filename},{key},{primary_host},{primary_port}\n")
    return output_path


def main() -> None:
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 6000
    folder_arg = sys.argv[3] if len(sys.argv) > 3 else None
    output_arg = sys.argv[4] if len(sys.argv) > 4 else None

    try:
        published = asyncio.run(publish_via_existing_node(host, port, folder_arg))
    except FileNotFoundError as exc:
        print(f"[-] {exc}", file=sys.stderr)
        return

    output_path = _write_output(published, output_arg)
    print(f"[+] Wrote metadata keys to {output_path}")


if __name__ == "__main__":
    main()