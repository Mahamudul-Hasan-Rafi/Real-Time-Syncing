"""Tiny TCP forwarder used to simulate outages: kill this process and the link is down.

    python tcp_proxy.py <listen port> <target host> <target port>
"""
import asyncio
import sys

listen, host, port = int(sys.argv[1]), sys.argv[2], int(sys.argv[3])


async def pump(reader, writer):
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except Exception:
        pass
    finally:
        writer.close()


async def handle(reader, writer):
    try:
        r2, w2 = await asyncio.open_connection(host, port)
    except OSError:
        writer.close()
        return
    await asyncio.gather(pump(reader, w2), pump(r2, writer))


async def main():
    server = await asyncio.start_server(handle, "127.0.0.1", listen)
    async with server:
        await server.serve_forever()


asyncio.run(main())
