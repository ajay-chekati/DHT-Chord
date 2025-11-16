# visualize_ring.py
"""
Interactive Chord ring visualizer.

Usage:
    python scripts/visualize_ring.py [--host 127.0.0.1] [--ports 6000 6001 ...]
    python scripts/visualize_ring.py --host 127.0.0.1 --bootstrap 6000

Displays a simple window showing the current ring layout. You can either
provide explicit ports or let the tool walk the ring starting from a single
bootstrap node. Clicking on a node reveals the metadata entries that node is
currently responsible for.
"""

import argparse
import asyncio
import math
import sys
import tkinter as tk
from collections import defaultdict
from functools import partial
from pathlib import Path
from typing import Dict, List, Optional, Tuple

SCRIPTS_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPTS_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import config  # type: ignore  # pylint: disable=import-error
from network import NodeInfo, RPCClient  # type: ignore  # pylint: disable=import-error


DEFAULT_PORTS = [6000, 6001, 6002, 6003, 6004, 6005]
PUBLISHED_KEYS_FILE = SCRIPTS_DIR / "published_keys.txt"


def load_published_entries(path: Path) -> List[Tuple[str, int]]:
    if not path.exists():
        return []
    entries: List[Tuple[str, int]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = [part.strip() for part in line.split(",")]
        if len(parts) < 2:
            continue
        filename = parts[0]
        try:
            key = int(parts[1])
        except ValueError:
            continue
        entries.append((filename, key))
    return entries


async def gather_nodes(targets: List[Tuple[str, int]]) -> List[Dict[str, object]]:
    client = RPCClient()
    nodes: List[Dict[str, object]] = []
    seen: set[Tuple[str, int]] = set()
    for host, port in targets:
        addr = (str(host), int(port))
        if addr in seen:
            continue
        seen.add(addr)
        record: Dict[str, object] = {"host": addr[0], "port": addr[1], "status": "down"}
        node_ref = NodeInfo(0, addr[0], addr[1])
        try:
            ping = await client.call(node_ref, {"type": "PING", "payload": {}}, timeout=config.RPC_TIMEOUT)
        except Exception:
            nodes.append(record)
            continue

        node_data = ping.get("result", {}).get("node")
        if not node_data:
            nodes.append(record)
            continue

        record["status"] = "up"
        record["id"] = int(node_data.get("id", 0))

        try:
            pred_resp = await client.call(node_ref, {"type": "GET_PREDECESSOR", "payload": {}}, timeout=config.RPC_TIMEOUT)
            record["predecessor"] = pred_resp.get("result", {}).get("node")
        except Exception:
            record["predecessor"] = None

        try:
            succ_resp = await client.call(node_ref, {"type": "GET_SUCCESSOR_LIST", "payload": {}}, timeout=config.RPC_TIMEOUT)
            chain = succ_resp.get("result", {}).get("nodes", [])
            record["successor"] = chain[0] if chain else None
        except Exception:
            record["successor"] = None

        nodes.append(record)

    return nodes


async def discover_ring(
    host: str,
    start_port: int,
    max_nodes: int = 512,
) -> List[Tuple[str, int]]:
    """
    Walk the ring starting from a single node and return the ordered list of addresses.
    """
    client = RPCClient()
    discovered: List[Tuple[str, int]] = []
    seen: set[Tuple[str, int]] = set()
    current_host = str(host)
    current_port = int(start_port)

    for _ in range(max_nodes):
        addr = (current_host, current_port)
        if addr in seen:
            break
        seen.add(addr)

        node_ref = NodeInfo(0, current_host, current_port)
        try:
            await client.call(node_ref, {"type": "PING", "payload": {}}, timeout=config.RPC_TIMEOUT)
        except Exception:
            # stop discovery if current node unreachable
            break

        discovered.append(addr)

        try:
            succ_resp = await client.call(
                node_ref,
                {"type": "GET_SUCCESSOR_LIST", "payload": {}},
                timeout=config.RPC_TIMEOUT,
            )
        except Exception:
            break

        chain = succ_resp.get("result", {}).get("nodes", [])
        successor = chain[0] if chain else None
        if not successor:
            break

        next_host = successor.get("host")
        next_port = successor.get("port")
        if next_host is None or next_port is None:
            break

        current_host = str(next_host)
        current_port = int(next_port)

    return discovered


async def collect_metadata_summary(
    alive_nodes: List[Dict[str, object]],
    published_entries: List[Tuple[str, int]],
) -> Dict[Tuple[str, int], List[Dict[str, object]]]:
    summary: Dict[Tuple[str, int], List[Dict[str, object]]] = defaultdict(list)
    if not alive_nodes or not published_entries:
        return summary

    primary = alive_nodes[0]
    base_node = NodeInfo(0, str(primary["host"]), int(primary["port"]))
    client = RPCClient()

    for filename, key in published_entries:
        try:
            resp = await client.call(
                base_node,
                {"type": "ROUTE_GET", "payload": {"key": key}},
                timeout=config.RPC_TIMEOUT,
            )
        except Exception:
            continue
        if not resp.get("ok"):
            continue

        result = resp.get("result", {}) or {}
        trace = result.get("trace") or []
        responsible: Optional[Tuple[str, int]] = None
        if trace:
            last = trace[-1]
            host_val = last.get("host")
            port_val = last.get("port")
            if host_val is not None and port_val is not None:
                responsible = (str(host_val), int(port_val))

        values = result.get("values") or []
        if not values or responsible is None:
            continue

        for meta in values:
            entry = {
                "key": key,
                "filename": meta.get("filename", filename),
                "file_host": meta.get("host"),
                "file_port": meta.get("port"),
                "size": meta.get("size"),
                "checksum": meta.get("checksum"),
            }
            summary[responsible].append(entry)

    return summary


class RingVisualizer:
    CANVAS_SIZE = 620
    NODE_RADIUS = 24
    TOOLTIP_TEXT = "Click a node to view metadata"

    def __init__(
        self,
        host: str,
        ports: Optional[List[int]],
        keys_file: Path,
        bootstrap_port: Optional[int],
        max_nodes: int,
    ):
        self.host = host
        self.manual_ports = ports
        self.bootstrap_port = bootstrap_port
        self.max_nodes = max_nodes
        self.keys_file = keys_file

        self.nodes: List[Dict[str, object]] = []
        self.metadata: Dict[Tuple[str, int], List[Dict[str, object]]] = {}
        self._current_targets: List[Tuple[str, int]] = []
        self.details_window: Optional[tk.Toplevel] = None

        self.root = tk.Tk()
        self.root.title("Chord Ring Visualizer")

        self.canvas = tk.Canvas(self.root, width=self.CANVAS_SIZE, height=self.CANVAS_SIZE, bg="white")
        self.canvas.pack(fill="both", expand=True, padx=10, pady=10)

        control_frame = tk.Frame(self.root)
        control_frame.pack(fill="x", padx=10, pady=(0, 10))
        tk.Button(control_frame, text="Refresh", command=self.refresh).pack(side="left")
        self.status_var = tk.StringVar(value="Ready")
        tk.Label(control_frame, textvariable=self.status_var, anchor="w").pack(side="left", padx=10)

    def run(self) -> None:
        self.refresh()
        self.root.mainloop()

    def refresh(self) -> None:
        self.status_var.set("Refreshing...")
        self.root.update_idletasks()
        try:
            asyncio.run(self._load_state())
            self.draw()
            self.status_var.set("Loaded ring state")
        except Exception as exc:
            self.canvas.delete("all")
            self.status_var.set(f"Error: {exc}")

    async def _load_state(self) -> None:
        if self.manual_ports:
            targets = [(self.host, int(port)) for port in self.manual_ports]
        elif self.bootstrap_port is not None:
            targets = await discover_ring(self.host, self.bootstrap_port, self.max_nodes)
            if not targets:
                targets = [(self.host, self.bootstrap_port)]
        else:
            targets = [(self.host, port) for port in DEFAULT_PORTS]

        self._current_targets = targets
        self.nodes = await gather_nodes(targets)
        published_entries = load_published_entries(self.keys_file)
        alive_nodes = [n for n in self.nodes if n.get("status") == "up"]
        self.metadata = await collect_metadata_summary(alive_nodes, published_entries)

    def draw(self) -> None:
        self.canvas.delete("all")
        alive = [n for n in self.nodes if n.get("status") == "up"]
        down = [n for n in self.nodes if n.get("status") != "up"]

        if self.details_window and self.details_window.winfo_exists():
            self.details_window.destroy()
            self.details_window = None

        center = self.CANVAS_SIZE / 2
        radius = (self.CANVAS_SIZE / 2) - 80

        self.tooltip = self.canvas.create_text(
            center,
            20,
            text=self.TOOLTIP_TEXT,
            font=("Helvetica", 11),
            fill="#424242",
        )

        if alive:
            ordered = sorted(alive, key=lambda n: int(n.get("id", 0)))
            count = len(ordered)

            # Draw ring circle
            self.canvas.create_oval(
                center - radius,
                center - radius,
                center + radius,
                center + radius,
                outline="#cccccc",
                dash=(4, 4),
            )

            for idx, node in enumerate(ordered):
                angle = (2 * math.pi * idx / count) - math.pi / 2
                x = center + radius * math.cos(angle)
                y = center + radius * math.sin(angle)

                host = str(node.get("host"))
                port = int(node.get("port"))
                label = f"{host}:{port}\nid={node.get('id')}"
                key = (host, port)
                has_metadata = key in self.metadata and len(self.metadata[key]) > 0

                fill_color = "#4caf50" if has_metadata else "#2196f3"
                oval = self.canvas.create_oval(
                    x - self.NODE_RADIUS,
                    y - self.NODE_RADIUS,
                    x + self.NODE_RADIUS,
                    y + self.NODE_RADIUS,
                    fill=fill_color,
                    outline="#1a237e",
                    width=2,
                )
                text_item = self.canvas.create_text(
                    x,
                    y,
                    text=str(port),
                    fill="white",
                    font=("Helvetica", 10, "bold"),
                )

                for item in (oval, text_item):
                    self.canvas.tag_bind(
                        item,
                        "<Button-1>",
                        partial(self.show_node_details, host=host, port=port),
                    )
                    self.canvas.tag_bind(
                        item,
                        "<Enter>",
                        partial(self._set_tooltip, text=label),
                    )
                    self.canvas.tag_bind(
                        item,
                        "<Leave>",
                        partial(self._set_tooltip, text=self.TOOLTIP_TEXT),
                    )

        else:
            self.canvas.create_text(
                center,
                center,
                text="No reachable nodes",
                font=("Helvetica", 16, "bold"),
                fill="#b71c1c",
            )

        if down:
            down_text = "Unreachable: " + ", ".join(f"{d['host']}:{d['port']}" for d in down)
            self.canvas.create_text(
                self.CANVAS_SIZE / 2,
                self.CANVAS_SIZE - 20,
                text=down_text,
                fill="#f57c00",
                font=("Helvetica", 10),
            )

    def _set_tooltip(self, _event=None, text: str = "") -> None:
        if hasattr(self, "tooltip"):
            self.canvas.itemconfig(self.tooltip, text=text)

    def show_node_details(self, _event, host: str, port: int) -> None:
        entries = self.metadata.get((host, port), [])

        if self.details_window and self.details_window.winfo_exists():
            self.details_window.destroy()

        window = tk.Toplevel(self.root)
        window.title(f"Node {host}:{port}")
        window.transient(self.root)
        window.geometry("480x360")
        window.minsize(360, 240)

        body = tk.Frame(window)
        body.pack(fill="both", expand=True)

        text_widget = tk.Text(body, wrap="word", font=("Helvetica", 11))
        scrollbar = tk.Scrollbar(body, command=text_widget.yview)
        text_widget.configure(yscrollcommand=scrollbar.set)
        text_widget.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        if entries:
            for entry in sorted(entries, key=lambda e: e["filename"]):
                size = entry.get("size")
                size_part = f" ({size} bytes)" if size is not None else ""
                file_host = entry.get("file_host")
                file_port = entry.get("file_port")
                checksum = entry.get("checksum")
                text_widget.insert(
                    "end",
                    f"Filename: {entry['filename']}{size_part}\n"
                    f"Key: {entry['key']}\n"
                    f"File origin: {file_host}:{file_port}\n"
                    f"Checksum: {checksum}\n\n",
                )
        else:
            text_widget.insert("end", "No metadata entries currently stored on this node.\n")

        text_widget.config(state="disabled")

        tk.Button(window, text="Close", command=window.destroy).pack(pady=6)
        window.focus_force()
        self.details_window = window


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Visualize the current Chord ring state.")
    parser.add_argument("--host", default="127.0.0.1", help="Host where nodes listen (default: 127.0.0.1)")
    parser.add_argument(
        "--ports",
        nargs="*",
        type=int,
        help="Ports to probe (default: 6000-6005)",
    )
    parser.add_argument(
        "--bootstrap",
        type=int,
        help="Bootstrap port to auto-discover the ring (ignored if --ports provided)",
    )
    parser.add_argument(
        "--max-nodes",
        type=int,
        default=512,
        help="Maximum nodes to discover when using --bootstrap",
    )
    parser.add_argument(
        "--keys-file",
        default=PUBLISHED_KEYS_FILE,
        type=Path,
        help="CSV file with filename,key,... entries (default: scripts/published_keys.txt)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ports = args.ports if args.ports else None
    app = RingVisualizer(
        host=args.host,
        ports=ports,
        keys_file=args.keys_file,
        bootstrap_port=args.bootstrap,
        max_nodes=args.max_nodes,
    )
    app.run()


if __name__ == "__main__":
    main()

