# network.py
import asyncio
import json
from typing import Callable, Awaitable, Dict, Any, Tuple
from dataclasses import dataclass

# A tiny JSON/TCP RPC server + client for our DHT
# Messages: {"type": "...", "payload": {...}}
# Responses: {"ok": True/False, "result": {...} | "error": "..."}

@dataclass(frozen=True)
class NodeInfo:
    id: int
    host: str
    port: int

# RPC Server ---------------------------------------------------------
class RPCServer:
    def __init__(self, host: str, port: int, handler: Callable[[Dict[str, Any], Tuple[str,int]], Awaitable[Dict[str,Any]]]):
        self.host = host
        self.port = port
        self._handler = handler
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._client_connected, self.host, self.port)
        # print server addr
        # print(f"RPCServer listening on {self.host}:{self.port}")

    async def _client_connected(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername")
        try:
            data = await reader.read(65536)
            if not data:
                writer.close()
                await writer.wait_closed()
                return
            try:
                msg = json.loads(data.decode())
            except Exception as e:
                resp = {"ok": False, "error": f"bad-json: {e}"}
                writer.write(json.dumps(resp).encode())
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                return
            try:
                out = await self._handler(msg, peer)
            except Exception as e:
                out = {"ok": False, "error": f"handler-exception: {e}"}
            writer.write(json.dumps(out).encode())
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

# RPC Client ---------------------------------------------------------
class RPCClient:
    async def call(self, node: NodeInfo, message: Dict[str, Any], timeout: float = 2.0) -> Dict[str, Any]:
        """
        One-shot JSON RPC call. Returns parsed dict. Raises on failure.
        """
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(node.host, node.port), timeout=timeout)
        except Exception as e:
            raise ConnectionError(f"connect-failed {node.host}:{node.port}: {e}")

        try:
            writer.write(json.dumps(message).encode())
            await writer.drain()
            data = await asyncio.wait_for(reader.read(65536), timeout=timeout)
            if not data:
                raise ConnectionError("no response")
            resp = json.loads(data.decode())
            return resp
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass