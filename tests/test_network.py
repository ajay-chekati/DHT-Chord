import asyncio

from network import NodeInfo, RPCClient, RPCServer

from .helpers import get_free_port


def test_rpc_server_client_roundtrip():
    async def scenario():
        responses = []

        async def handler(message, peer):
            responses.append((message, peer))
            payload = message.get("payload", {})
            return {"ok": True, "result": {"echo": payload.get("value")}}

        port = get_free_port()
        server = RPCServer("127.0.0.1", port, handler)
        await server.start()
        try:
            client = RPCClient()
            target = NodeInfo(0, "127.0.0.1", port)
            resp = await client.call(target, {"type": "ECHO", "payload": {"value": 123}})
            assert resp["ok"]
            assert resp["result"]["echo"] == 123
            assert responses
            assert responses[0][0]["type"] == "ECHO"
        finally:
            await server.stop()

    asyncio.run(scenario())
