# node.py
import asyncio
import time
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, asdict
from network import NodeInfo, RPCServer, RPCClient
import config
from utils import hash_bytes_to_int, in_interval
import logging
logger = logging.getLogger("chord")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

# ---- Metadata stored in DHT ----
@dataclass
class Metadata:
    host: str
    port: int
    filename: str
    size: int
    checksum: str

    def to_dict(self) -> Dict[str,Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str,Any]) -> 'Metadata':
        return Metadata(**d)

# ---- Routing table (fingers) ----
class RoutingTable:
    def __init__(self, self_id: int, m: int = config.M):
        self.self_id = self_id
        self.m = m
        # fingers[0] is finger 1 (2^0), up to m entries
        self.fingers: List[Optional[NodeInfo]] = [None] * m

    def set_finger(self, i: int, node: NodeInfo) -> None:
        if 1 <= i <= self.m:
            self.fingers[i-1] = node

    def get_finger(self, i: int) -> Optional[NodeInfo]:
        if 1 <= i <= self.m:
            return self.fingers[i-1]
        return None

    def closest_preceding_finger(self, target_id: int) -> Optional[NodeInfo]:
        # search fingers high->low
        for node in reversed(self.fingers):
            if node is None:
                continue
            if in_interval(self.self_id, target_id, node.id, inclusive_start=False, inclusive_end=False, m=self.m):
                return node
        return None

# ---- Successor list ----
class SuccessorList:
    def __init__(self, capacity: int = config.REPLICATION):
        self.capacity = capacity
        self._list: List[NodeInfo] = []

    def update_with(self, node: NodeInfo) -> None:
        # remove existing duplicate
        self._list = [n for n in self._list if not (n.host == node.host and n.port == node.port)]
        # insert at front
        self._list.insert(0, node)
        # trim
        self._list = self._list[:self.capacity]

    def primary(self) -> Optional[NodeInfo]:
        return self._list[0] if self._list else None

    def all(self) -> List[NodeInfo]:
        return list(self._list)

    def replace(self, nodes: List[NodeInfo]) -> None:
        dedup: List[NodeInfo] = []
        seen = set()
        for node in nodes:
            if node is None:
                continue
            key = (node.host, node.port)
            if key in seen:
                continue
            seen.add(key)
            dedup.append(node)
        self._list = dedup[:self.capacity]

# ---- Local storage ----
class Storage:
    def __init__(self):
        # key (int) -> list of metadata dicts
        self._store: Dict[int, List[Dict[str,Any]]] = {}

    def put(self, key: int, meta: Metadata) -> None:
        self._store.setdefault(key, [])
        # de-dupe by host:port:filename
        entry = meta.to_dict()
        if not any(e['host'] == entry['host'] and e['port'] == entry['port'] and e['filename'] == entry['filename'] for e in self._store[key]):
            self._store[key].append(entry)

    def get(self, key: int) -> Optional[List[Metadata]]:
        if key not in self._store:
            return None
        return [Metadata.from_dict(d) for d in self._store[key]]

    def remove_host(self, host: str, port: int) -> None:
        for k in list(self._store.keys()):
            self._store[k] = [d for d in self._store[k] if not (d['host'] == host and d['port'] == port)]
            if not self._store[k]:
                del self._store[k]

    def keys_in_range(self, start: int, end: int, m: int = config.M) -> Dict[int, List[Dict[str,Any]]]:
        out: Dict[int, List[Dict[str,Any]]] = {}
        for k, v in self._store.items():
            if in_interval(start, end, k, inclusive_start=False, inclusive_end=True, m=m):
                out[k] = v
        return out

    def take_keys_in_range(self, start: int, end: int, m: int = config.M) -> Dict[int, List[Dict[str,Any]]]:
        """
        Remove and return keys in (start, end] on the identifier circle.
        """
        taken: Dict[int, List[Dict[str,Any]]] = {}
        for k in list(self._store.keys()):
            if in_interval(start, end, k, inclusive_start=False, inclusive_end=True, m=m):
                # copy value to avoid accidental shared mutation
                taken[k] = [dict(entry) for entry in self._store[k]]
                del self._store[k]
        return taken

    def all_items(self) -> Dict[int, List[Dict[str,Any]]]:
        return {k: [dict(entry) for entry in v] for k, v in self._store.items()}

    def pop_all(self) -> Dict[int, List[Dict[str,Any]]]:
        data = self.all_items()
        self._store.clear()
        return data

# ---- Node class ----
class Node:
    def __init__(self, host: str, port: int, bootstrap: Optional[NodeInfo] = None):
        self.host = host
        self.port = port
        self.id = hash_bytes_to_int(f"{host}:{port}".encode(), config.M)
        self.info = NodeInfo(self.id, host, port)

        # pointers
        self.successor: Optional[NodeInfo] = None
        self.predecessor: Optional[NodeInfo] = None

        self.routing = RoutingTable(self.id)
        self.succ_list = SuccessorList()
        self.storage = Storage()
        self.local_publications: Dict[int, Dict[str, Metadata]] = {}

        # network
        self.rpc_server: Optional[RPCServer] = None
        self.rpc_client = RPCClient()

        # maintenance
        self._tasks: List[asyncio.Task] = []
        self._stop = False

        self.bootstrap = bootstrap

    # ---------------- network handler ----------------
    async def _rpc_handler(self, msg: Dict[str,Any], sender: tuple) -> Dict[str,Any]:
        t = msg.get("type")
        p = msg.get("payload", {})
        try:
            if t == "PING":
                return {"ok": True, "result": {"type": "PONG", "node": {"id": self.id, "host": self.host, "port": self.port}}}
            if t == "FIND_SUCCESSOR":
                target = int(p["id"])
                succ, trace = await self._find_successor_internal(target)
                trace_payload = [self._nodeinfo_to_dict(n) for n in trace]
                return {
                    "ok": True,
                    "result": {
                        "node": {"id": succ.id, "host": succ.host, "port": succ.port},
                        "hops": max(0, len(trace_payload) - 1),
                        "trace": trace_payload,
                    },
                }
            if t == "GET_PREDECESSOR":
                if self.predecessor:
                    return {"ok": True, "result": {"node": {"id": self.predecessor.id, "host": self.predecessor.host, "port": self.predecessor.port}}}
                return {"ok": True, "result": {"node": None}}
            if t == "NOTIFY":
                node = p["node"]
                cand = NodeInfo(int(node["id"]), node["host"], node["port"])
                await self.notify(cand)
                return {"ok": True, "result": {}}
            if t == "PUT":
                key = int(p["key"])
                meta = Metadata.from_dict(p["meta"])
                self.storage.put(key, meta)
                return {"ok": True, "result": {}}
            if t == "GET":
                key = int(p["key"])
                vals = self.storage.get(key)
                if vals is None:
                    return {"ok": True, "result": {"values": None}}
                return {"ok": True, "result": {"values": [v.to_dict() for v in vals]}}
            if t == "ROUTE_PUT":
                key = int(p["key"])
                meta = Metadata.from_dict(p["meta"])
                primary, trace, replicas = await self.put_with_trace(key, meta)
                return {
                    "ok": True,
                    "result": {
                        "key": key,
                        "primary": self._nodeinfo_to_dict(primary),
                        "trace": [self._nodeinfo_to_dict(n) for n in trace],
                        "replicas": [self._nodeinfo_to_dict(n) for n in replicas],
                    },
                }
            if t == "ROUTE_GET":
                key = int(p["key"])
                vals, trace = await self.get_with_trace(key)
                payload = {
                    "values": None if vals is None else [v.to_dict() for v in vals],
                    "trace": [self._nodeinfo_to_dict(n) for n in trace],
                }
                return {"ok": True, "result": payload}
            if t == "GET_SUCCESSOR_LIST":
                nodes = [self._nodeinfo_to_dict(n) for n in self._current_successor_chain()]
                return {"ok": True, "result": {"nodes": [n for n in nodes if n is not None]}}
            if t == "TRANSFER_REQUEST":
                start = int(p["start"])
                end = int(p["end"])
                taken = self.storage.take_keys_in_range(start, end, self.routing.m)
                payload = [{"key": key, "values": values} for key, values in taken.items()]
                return {"ok": True, "result": {"keys": payload}}
            if t == "TRANSFER_KEYS":
                items = p.get("keys", [])
                await self._ingest_transferred_keys(items)
                return {"ok": True, "result": {}}
            if t == "SET_PREDECESSOR":
                node = self._nodeinfo_from_dict(p.get("node"))
                self.predecessor = node
                return {"ok": True, "result": {}}
            if t == "SET_SUCCESSOR":
                node = self._nodeinfo_from_dict(p.get("node"))
                self.successor = node
                if node:
                    self.succ_list.update_with(node)
                return {"ok": True, "result": {}}
            return {"ok": False, "error": f"unknown-type {t}"}
        except Exception as e:
            return {"ok": False, "error": f"exception {e}"}

    async def _ingest_transferred_keys(self, items: List[Dict[str,Any]]) -> None:
        for entry in items:
            try:
                key = int(entry["key"])
            except (KeyError, ValueError, TypeError):
                continue
            values = entry.get("values", [])
            if not isinstance(values, list):
                continue
            for raw in values:
                try:
                    meta = Metadata.from_dict(raw)
                except Exception:
                    continue
                self.storage.put(key, meta)

    async def _request_keys_from_successor(self, start_id: int, end_id: int) -> None:
        if self.successor is None or self.successor == self.info:
            return
        try:
            resp = await self.rpc_client.call(
                self.successor,
                {"type": "TRANSFER_REQUEST", "payload": {"start": start_id, "end": end_id}},
                timeout=config.RPC_TIMEOUT,
            )
        except Exception:
            return
        if not resp.get("ok"):
            return
        items = resp.get("result", {}).get("keys", [])
        await self._ingest_transferred_keys(items)

    def _nodeinfo_from_dict(self, data: Optional[Dict[str,Any]]) -> Optional[NodeInfo]:
        if not data:
            return None
        try:
            return NodeInfo(int(data["id"]), data["host"], data["port"])
        except Exception:
            return None

    def _nodeinfo_to_dict(self, node: Optional[NodeInfo]) -> Optional[Dict[str,Any]]:
        if node is None:
            return None
        return {"id": node.id, "host": node.host, "port": node.port}

    def _current_successor_chain(self) -> List[NodeInfo]:
        chain: List[NodeInfo] = []
        seen = set()
        for node in [self.successor] + self.succ_list.all():
            if node is None:
                continue
            key = (node.id, node.host, node.port)
            if key in seen:
                continue
            seen.add(key)
            chain.append(node)
        return chain

   
    def _owns_primary_responsibility(self, key: int) -> bool:
        # If predecessor is set use it; otherwise use a conservative fallback
        if self.predecessor:
            start = self.predecessor.id
        else:
            start = (self.id - 1) % (1 << self.routing.m)
        return in_interval(start, self.id, key, inclusive_start=False, inclusive_end=True, m=self.routing.m)

    def _publication_key(self, meta: Metadata) -> str:
        return f"{meta.host}:{meta.port}:{meta.filename}"

    def _register_local_publication(self, key: int, meta: Metadata) -> None:
        if meta.host != self.host or meta.port != self.port:
            return
        bucket = self.local_publications.setdefault(key, {})
        bucket[self._publication_key(meta)] = meta

    def _refresh_successor_list_from_chain(self, chain: List[NodeInfo]) -> None:
        filtered = [node for node in chain if node != self.info]
        self.succ_list.replace(filtered)

    def _candidate_forward_nodes(self, target_id: int) -> List[NodeInfo]:
        candidates: List[NodeInfo] = []
        seen = set()
        if self.successor and self.successor != self.info:
            seen.add((self.successor.id, self.successor.host, self.successor.port))
            candidates.append(self.successor)
        for node in reversed(self.routing.fingers):
            if node is None or node == self.info:
                continue
            if not in_interval(self.id, target_id, node.id, inclusive_start=False, inclusive_end=False, m=self.routing.m):
                continue
            key = (node.id, node.host, node.port)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(node)
        for node in self.succ_list.all():
            if node is None or node == self.info:
                continue
            key = (node.id, node.host, node.port)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(node)
        return candidates

    async def _resolve_bootstrap(self, node: NodeInfo) -> NodeInfo:
        try:
            resp = await self.rpc_client.call(node, {"type": "PING", "payload": {}}, timeout=config.RPC_TIMEOUT)
            if resp.get("ok"):
                info = self._nodeinfo_from_dict(resp.get("result", {}).get("node"))
                if info:
                    return info
        except Exception:
            pass
        return node

    async def _fetch_successor_chain(self, node: NodeInfo) -> List[NodeInfo]:
        if node == self.info:
            return self._current_successor_chain()
        try:
            resp = await self.rpc_client.call(
                node, {"type": "GET_SUCCESSOR_LIST", "payload": {}}, timeout=config.RPC_TIMEOUT
            )
            if resp.get("ok"):
                chain: List[NodeInfo] = []
                for entry in resp.get("result", {}).get("nodes", []):
                    ni = self._nodeinfo_from_dict(entry)
                    if ni:
                        chain.append(ni)
                if not chain or chain[0] != node:
                    chain.insert(0, node)
                return self._dedup_chain(chain)
        except Exception:
            pass
        fallback = [node] + self.succ_list.all()
        return self._dedup_chain(fallback)

    def _dedup_chain(self, nodes: List[Optional[NodeInfo]]) -> List[NodeInfo]:
        out: List[NodeInfo] = []
        seen = set()
        for n in nodes:
            if n is None:
                continue
            key = (n.id, n.host, n.port)
            if key in seen:
                continue
            seen.add(key)
            out.append(n)
        return out

    def _merge_trace(self, base: List[NodeInfo], extra: List[NodeInfo]) -> List[NodeInfo]:
        out: List[NodeInfo] = list(base)
        seen = {(node.id, node.host, node.port) for node in out}
        for node in extra:
            if node is None:
                continue
            key = (node.id, node.host, node.port)
            if key in seen:
                continue
            seen.add(key)
            out.append(node)
        return out

    def _successor_candidates(self) -> List[NodeInfo]:
        candidates: List[NodeInfo] = []
        seen = set()
        primary = self.successor
        if primary is not None:
            seen.add((primary.id, primary.host, primary.port))
            if primary != self.info:
                candidates.append(primary)
        for node in self.succ_list.all():
            if node is None or node == self.info:
                continue
            key = (node.id, node.host, node.port)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(node)
        return candidates

    async def leave(self) -> None:
        """
        Gracefully hand off stored keys to a successor and notify neighbors.
        """
        candidates = self._successor_candidates()
        if not candidates:
            return

        data = self.storage.pop_all()
        items = [{"key": key, "values": values} for key, values in data.items()]

        target: Optional[NodeInfo] = None
        for candidate in candidates:
            try:
                if items:
                    await self.rpc_client.call(
                        candidate,
                        {"type": "TRANSFER_KEYS", "payload": {"keys": items}},
                        timeout=config.RPC_TIMEOUT,
                    )
                else:
                    await self.rpc_client.call(
                        candidate,
                        {"type": "PING", "payload": {}},
                        timeout=config.RPC_TIMEOUT,
                    )
                target = candidate
                break
            except Exception:
                continue

        if target is None:
            if items:
                # re-ingest the data we attempted to hand off
                await self._ingest_transferred_keys(items)
            return

        # notify successor to update predecessor pointer
        predecessor_payload = None
        if self.predecessor and self.predecessor != self.info:
            predecessor_payload = {"id": self.predecessor.id, "host": self.predecessor.host, "port": self.predecessor.port}

        try:
            await self.rpc_client.call(
                target,
                {"type": "SET_PREDECESSOR", "payload": {"node": predecessor_payload}},
                timeout=config.RPC_TIMEOUT,
            )
        except Exception:
            pass

        # notify predecessor to update successor pointer
        if self.predecessor and self.predecessor != self.info:
            successor_payload = {"id": target.id, "host": target.host, "port": target.port}
            try:
                await self.rpc_client.call(
                    self.predecessor,
                    {"type": "SET_SUCCESSOR", "payload": {"node": successor_payload}},
                    timeout=config.RPC_TIMEOUT,
                )
            except Exception:
                pass

    # ---------------- startup / shutdown ----------------
    async def start(self) -> None:
        self.rpc_server = RPCServer(self.host, self.port, self._rpc_handler)
        await self.rpc_server.start()
        # initialize pointers if no bootstrap
        if self.bootstrap is None:
            # create single-node ring
            self.successor = self.info
            self.predecessor = None
            self.succ_list.update_with(self.info)
        else:
            # join using bootstrap async
            await self.join(self.bootstrap)

        # schedule maintenance tasks
        loop = asyncio.get_event_loop()
        self._tasks.append(loop.create_task(self._stabilize_loop()))
        self._tasks.append(loop.create_task(self._fix_fingers_loop()))
        self._tasks.append(loop.create_task(self._check_predecessor_loop()))
        self._tasks.append(loop.create_task(self._repair_replicas_loop()))
        self._tasks.append(loop.create_task(self._republish_loop()))

    async def stop(self, graceful: bool = True) -> None:
        if graceful:
            try:
                await self.leave()
            except Exception:
                pass
        self._stop = True
        for t in list(self._tasks):
            t.cancel()
        if self.rpc_server:
            await self.rpc_server.stop()

    # ---------------- join/find ----------------
    async def join(self, bootstrap: NodeInfo) -> None:
        bootstrap = await self._resolve_bootstrap(bootstrap)
        # ask bootstrap for successor of our id
        msg = {"type": "FIND_SUCCESSOR", "payload": {"id": self.id}}
        try:
            resp = await self.rpc_client.call(bootstrap, msg, timeout=config.RPC_TIMEOUT)
            if resp.get("ok"):
                n = resp["result"]["node"]
                self.successor = NodeInfo(int(n["id"]), n["host"], n["port"])
                # update successor list
                self.succ_list.update_with(self.successor)
                try:
                    chain = await self._fetch_successor_chain(self.successor)
                    self._refresh_successor_list_from_chain(chain)
                except Exception:
                    pass
            else:
                raise RuntimeError("bootstrap find_successor failed")
        except Exception as e:
            raise RuntimeError(f"join failed: {e}")

        # discover predecessor of successor (our predecessor)
        predecessor: Optional[NodeInfo] = None
        try:
            resp = await self.rpc_client.call(
                self.successor, {"type": "GET_PREDECESSOR", "payload": {}}, timeout=config.RPC_TIMEOUT
            )
            if resp.get("ok"):
                predecessor = self._nodeinfo_from_dict(resp.get("result", {}).get("node"))
        except Exception:
            predecessor = None

        if predecessor is not None and predecessor.id == self.id:
            predecessor = None

        self.predecessor = predecessor

        # proactively notify successor to accelerate stabilization
        try:
            await self.rpc_client.call(
                self.successor,
                {"type": "NOTIFY", "payload": {"node": {"id": self.id, "host": self.host, "port": self.port}}},
                timeout=config.RPC_TIMEOUT,
            )
        except Exception:
            pass

        # request key transfer for keys we now own
        if predecessor is not None:
            start_id = predecessor.id
        else:
            # conservative fallback: ask for everything up to self.id; successor will filter
            start_id = (self.id - 1) % (1 << self.routing.m)
        await self._request_keys_from_successor(start_id, self.id)

    async def _find_successor_internal(self, id_: int) -> Tuple[NodeInfo, List[NodeInfo]]:
        trace: List[NodeInfo] = [self.info]
        if self.successor and in_interval(self.id, self.successor.id, id_, inclusive_start=False, inclusive_end=True, m=self.routing.m):
            succ = self.successor
            if succ != self.info:
                trace.append(succ)
            return succ, trace

        attempted = set()
        payload = {"type": "FIND_SUCCESSOR", "payload": {"id": id_}}

        for _ in range(config.MAX_RPC_RETRIES + 1):
            candidates = self._candidate_forward_nodes(id_)
            for candidate in candidates:
                key = (candidate.id, candidate.host, candidate.port)
                if key in attempted:
                    continue
                attempted.add(key)
                try:
                    resp = await self.rpc_client.call(candidate, payload, timeout=config.RPC_TIMEOUT)
                except Exception:
                    continue
                if resp.get("ok"):
                    node = self._nodeinfo_from_dict(resp.get("result", {}).get("node"))
                    if node:
                        remote_payload = resp.get("result", {}).get("trace")
                        remote_trace: List[NodeInfo] = []
                        if isinstance(remote_payload, list):
                            for entry in remote_payload:
                                ni = self._nodeinfo_from_dict(entry)
                                if ni:
                                    remote_trace.append(ni)
                        if not remote_trace:
                            remote_trace = [candidate]
                            if node != candidate:
                                remote_trace.append(node)
                        combined_trace = self._merge_trace(trace, remote_trace)
                        if node and (not combined_trace or combined_trace[-1] != node):
                            combined_trace.append(node)
                        return node, combined_trace
            # if no candidates succeeded, loop to retry with refreshed data

        if self.successor:
            succ = self.successor
            if succ != self.info:
                trace.append(succ)
            if succ and (not trace or trace[-1] != succ):
                trace.append(succ)
            return succ, trace
        if not trace or trace[-1] != self.info:
            trace.append(self.info)
        return self.info, trace

    async def find_successor_with_trace(self, id_: int) -> Tuple[NodeInfo, List[NodeInfo]]:
        return await self._find_successor_internal(id_)

    async def find_successor(self, id_: int) -> NodeInfo:
        node, _ = await self.find_successor_with_trace(id_)
        return node

    # ---------------- maintenance loops ----------------
    async def _stabilize_loop(self):
        while not self._stop:
            try:
                await self.stabilize()
            except Exception:
                pass
            await asyncio.sleep(config.STABILIZE_INTERVAL)

    async def stabilize(self):
        """
        Ask successor for its predecessor x. If x is between self and successor, set successor=x.
        Then notify successor.
        """
        succ = self.successor
        if succ is None:
            return
        try:
            resp = await self.rpc_client.call(succ, {"type": "GET_PREDECESSOR", "payload": {}}, timeout=config.RPC_TIMEOUT)
            if resp.get("ok"):
                node = resp["result"]["node"]
                adopted = False
                if node:
                    x = NodeInfo(int(node["id"]), node["host"], node["port"])
                    # handle degenerate self-successor case (single-node ring)
                    if succ == self.info and x != self.info:
                        self.successor = x
                        adopted = True
                    # if x is between self and successor, adopt x
                    elif in_interval(self.id, succ.id, x.id, inclusive_start=False, inclusive_end=False, m=self.routing.m):
                        self.successor = x
                        adopted = True
                if adopted:
                    self.succ_list.update_with(self.successor)
                    succ = self.successor

            successor_for_notify = self.successor
            if successor_for_notify and successor_for_notify != self.info:
                # notify successor that I might be its predecessor
                await self.rpc_client.call(
                    successor_for_notify,
                    {"type": "NOTIFY", "payload": {"node": {"id": self.id, "host": self.host, "port": self.port}}},
                    timeout=config.RPC_TIMEOUT,
                )
            try:
                if self.successor:
                    chain = await self._fetch_successor_chain(self.successor)
                else:
                    chain = []
                self._refresh_successor_list_from_chain(chain)
            except Exception:
                if self.successor:
                    self.succ_list.update_with(self.successor)
        except Exception:
            # successor failed -> try next in succ_list
            for candidate in self.succ_list.all()[1:]:
                try:
                    await self.rpc_client.call(candidate, {"type": "PING", "payload": {}}, timeout=config.RPC_TIMEOUT)
                    self.successor = candidate
                    self.succ_list.update_with(candidate)
                    break
                except Exception:
                    continue

    async def notify(self, candidate: NodeInfo):
        """
        Called by other node saying "I might be your predecessor".
        If you have no predecessor or candidate is between your predecessor and you, update.
        Also: upon a graceful leave/transfer you'd implement key transfer here.
        """
        if candidate == self.info:
            return
        if self.predecessor is None:
            self.predecessor = candidate
        else:
            if in_interval(self.predecessor.id, self.id, candidate.id, inclusive_start=False, inclusive_end=False, m=self.routing.m):
                self.predecessor = candidate

    async def _fix_fingers_loop(self):
        i = 1
        while not self._stop:
            try:
                await self.fix_fingers(i)
            except Exception:
                pass
            i += 1
            if i > self.routing.m:
                i = 1
            await asyncio.sleep(config.FIX_FINGERS_INTERVAL)

    async def fix_fingers(self, i: int):
        """
        Recompute finger i: successor(self + 2^(i-1))
        """
        target = (self.id + (1 << (i-1))) % (1 << self.routing.m)
        try:
            succ = await self.find_successor(target)
            self.routing.set_finger(i, succ)
        except Exception:
            # ignore failure; will be retried
            pass

    async def _check_predecessor_loop(self):
        while not self._stop:
            try:
                await self.check_predecessor()
            except Exception:
                pass
            await asyncio.sleep(config.CHECK_PREDECESSOR_INTERVAL)

    async def check_predecessor(self):
        if self.predecessor is None:
            return
        try:
            await self.rpc_client.call(self.predecessor, {"type": "PING", "payload": {}}, timeout=config.RPC_TIMEOUT)
        except Exception:
            # predecessor failed
            self.predecessor = None

    async def _repair_replicas_loop(self):
        while not self._stop:
            try:
                await self.repair_replicas()
            except Exception:
                pass
            await asyncio.sleep(config.REPAIR_REPLICAS_INTERVAL)

    async def repair_replicas(self):
        stored = self.storage.all_items()
        if not stored:
            return
        chain = self._current_successor_chain()
        if not chain:
            return
        replication_targets = [
            node for node in chain[1 : max(1, config.REPLICATION)] if node != self.info
        ]

        for key, value_dicts in stored.items():
            if not self._owns_primary_responsibility(key):
                continue
            metas = []
            for raw in value_dicts:
                try:
                    metas.append(Metadata.from_dict(raw))
                except Exception:
                    continue
            if not metas:
                continue
            payloads = [{"type": "PUT", "payload": {"key": key, "meta": meta.to_dict()}} for meta in metas]
            for replica in replication_targets:
                for message in payloads:
                    try:
                        await self.rpc_client.call(replica, message, timeout=config.RPC_TIMEOUT)
                    except Exception:
                        continue

    async def _republish_loop(self):
        while not self._stop:
            try:
                await self.republish_owned_metadata()
            except Exception:
                pass
            await asyncio.sleep(config.REPUBLISH_INTERVAL)

    async def republish_owned_metadata(self):
        if not self.local_publications:
            return
        for key, entries in list(self.local_publications.items()):
            metas = list(entries.values())
            for meta in metas:
                try:
                    await self.put(key, meta)
                except Exception:
                    continue

    # ---------------- DHT ops ----------------
    async def _put(
        self, key: int, meta: Metadata, include_trace: bool = False
    ) -> Tuple[NodeInfo, List[NodeInfo], List[NodeInfo]]:
        """
        Internal PUT that optionally collects routing trace information.
        Returns tuple of (primary_node, trace, replication_targets).
        """
        self._register_local_publication(key, meta)
        if include_trace:
            target, trace = await self.find_successor_with_trace(key)
        else:
            target = await self.find_successor(key)
            trace = []

        payload = {"type": "PUT", "payload": {"key": key, "meta": meta.to_dict()}}

        primary_written = False
        primary_node = target
        if target == self.info:
            self.storage.put(key, meta)
            primary_written = True
        else:
            try:
                resp = await self.rpc_client.call(target, payload, timeout=config.RPC_TIMEOUT)
                if resp.get("ok"):
                    primary_written = True
            except Exception:
                primary_written = False

        if not primary_written:
            # attempt local write if we appear responsible
            if self._owns_primary_responsibility(key):
                self.storage.put(key, meta)
                primary_written = True
                primary_node = self.info
            else:
                raise RuntimeError("failed to write primary replica")

        if include_trace:
            trace = self._merge_trace(trace, [primary_node])

        # replicate to additional successors
        try:
            chain = await self._fetch_successor_chain(primary_node)
        except Exception:
            chain = [primary_node]
        # extend with local knowledge in case remote chain short
        for extra in self._current_successor_chain():
            if extra not in chain:
                chain.append(extra)
        chain = self._dedup_chain(chain)

        replication_targets = [
            node for node in chain[1:] if node != self.info
        ][: max(0, config.REPLICATION - 1)]

        for replica in replication_targets:
            try:
                await self.rpc_client.call(replica, payload, timeout=config.RPC_TIMEOUT)
            except Exception:
                # background repair will retry
                continue

        return primary_node, trace, replication_targets

    async def put_with_trace(self, key: int, meta: Metadata) -> Tuple[NodeInfo, List[NodeInfo], List[NodeInfo]]:
        return await self._put(key, meta, include_trace=True)

    async def put(self, key: int, meta: Metadata) -> None:
        """
        Route to successor and store metadata there. Also replicate to next r-1 successors.
        """
        await self._put(key, meta, include_trace=False)

    async def get_with_trace(self, key: int) -> Tuple[Optional[List[Metadata]], List[NodeInfo]]:
        """
        Find successor, retrieve metadata, and include the routing trace.
        """
        target, trace = await self.find_successor_with_trace(key)
        trace_nodes: List[NodeInfo] = list(trace)
        seen = {(node.id, node.host, node.port) for node in trace_nodes}

        def add_trace(node: Optional[NodeInfo], *, force: bool = False) -> None:
            if node is None:
                return
            key_tuple = (node.id, node.host, node.port)
            if force or key_tuple not in seen:
                trace_nodes.append(node)
                seen.add(key_tuple)

        add_trace(target, force=True)
        try:
            resp = await self.rpc_client.call(target, {"type": "GET", "payload": {"key": key}}, timeout=config.RPC_TIMEOUT)
        except Exception:
            resp = None

        if resp and resp.get("ok"):
            vals = resp["result"]["values"]
            if vals:
                return [Metadata.from_dict(d) for d in vals], trace_nodes
            for successor in self.succ_list.all()[1:]:
                add_trace(successor)
                try:
                    r2 = await self.rpc_client.call(successor, {"type": "GET", "payload": {"key": key}}, timeout=config.RPC_TIMEOUT)
                except Exception:
                    continue
                if r2.get("ok") and r2["result"]["values"]:
                    return [Metadata.from_dict(d) for d in r2["result"]["values"]], trace_nodes
            return None, trace_nodes

        local = self.storage.get(key)
        if local:
            return local, trace_nodes

        for successor in self.succ_list.all():
            add_trace(successor)
            try:
                r2 = await self.rpc_client.call(successor, {"type": "GET", "payload": {"key": key}}, timeout=config.RPC_TIMEOUT)
            except Exception:
                continue
            if r2.get("ok") and r2["result"]["values"]:
                return [Metadata.from_dict(d) for d in r2["result"]["values"]], trace_nodes

        return None, trace_nodes

    async def get(self, key: int) -> Optional[List[Metadata]]:
        """
        Find successor and ask for key. If not found, try successors in successor list.
        """
        values, _ = await self.get_with_trace(key)
        return values

    # ---------------- debug / helpers ----------------
    def info_str(self) -> str:
        succ = f"{self.successor.host}:{self.successor.port}" if self.successor else "None"
        pred = f"{self.predecessor.host}:{self.predecessor.port}" if self.predecessor else "None"
        return f"Node {self.host}:{self.port} id={self.id} succ={succ} pred={pred}"