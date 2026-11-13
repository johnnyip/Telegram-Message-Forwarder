"""Operator-only read-only IPC: inspect the existing client without a second auth owner."""
import asyncio
import json
import os
from pathlib import Path
import stat
from .input import parse_url
from .source import scopes
from .filesystems import validate as validate_filesystems


def summary(snap):
    values = snap["main"] + snap["discussion"]
    return {"peer": snap["peer"], "post": snap["post"], "thread": snap["thread"],
            "counts": {k:len(v) for k,v in scopes(snap).items()},
            "photos": sum(i["kind"] == "photo" for i in values),
            "videos": sum(i["kind"] == "video" for i in values),
            "partial": snap["partial"], "warning": snap["warning"]}


def socket_path(config):
    return Path(str(config.db)+".inspect.sock")


def remove_owned_socket(path, identity=None):
    try:
        st = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISSOCK(st.st_mode) or st.st_uid != os.getuid():
        raise RuntimeError("Inspection socket path is not an owned socket")
    if identity and (st.st_dev,st.st_ino) != identity:
        raise RuntimeError("Inspection socket ownership changed")
    path.unlink()


async def serve(config, resolver):
    path = socket_path(config)
    # Caller already owns the exclusive state lock; no other state-owner can be live.
    remove_owned_socket(path)
    semaphore = asyncio.Semaphore(1)
    async def handle(reader, writer):
        try:
            line = await asyncio.wait_for(reader.readline(), 5)
            request = json.loads(line)
            if isinstance(request,dict) and set(request) == {"health"} and request["health"] is True:
                validate_filesystems(config)
                writer.write(b'{"status":"ok"}\n')
                await writer.drain()
                return
            if not isinstance(request,dict) or set(request) != {"inspect"}:
                raise ValueError("invalid inspection request")
            ref = parse_url(request["inspect"])
            async with semaphore:
                result = summary(await resolver.resolve(ref))
            writer.write((json.dumps(result,ensure_ascii=False)+"\n").encode())
            await writer.drain()
        except Exception as exc:
            writer.write((json.dumps({"error":type(exc).__name__})+"\n").encode())
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
    server = await asyncio.start_unix_server(handle,path=str(path),limit=4096)
    os.chmod(path,0o600)
    st = path.lstat()
    try:
        async with server:
            await server.serve_forever()
    finally:
        remove_owned_socket(path,(st.st_dev,st.st_ino))


async def inspect_running(config, url):
    parse_url(url)
    path = socket_path(config)
    if not path.exists():
        return None
    reader, writer = await asyncio.open_unix_connection(str(path),limit=65536)
    try:
        writer.write((json.dumps({"inspect":url})+"\n").encode())
        await writer.drain()
        return json.loads(await asyncio.wait_for(reader.readline(),300))
    finally:
        writer.close()
        await writer.wait_closed()


async def health_running(config):
    reader, writer = await asyncio.open_unix_connection(str(socket_path(config)),limit=4096)
    try:
        writer.write(b'{"health":true}\n')
        await writer.drain()
        result = json.loads(await asyncio.wait_for(reader.readline(),5))
        if result != {"status":"ok"}:
            raise RuntimeError("Control health check failed")
        return result
    finally:
        writer.close()
        await writer.wait_closed()
