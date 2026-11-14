import asyncio
import uuid
import os
from ..spool_lifecycle import manager
from ..shared_media import _io,_join
from pathlib import Path
from typing import Any, List, Optional

from ..shared_media import configured_bridge, message_identity
from ..domain.media import build_filename_from_message, get_media_size, media_type
from ..core.metrics import StepTimer
from ..core.utils import tstamp


async def snapshot_media_message(msg, info: dict, source_kind: str, spool_dir: Path, download_semaphore, run_api, log) -> Optional[dict]:
    timer = StepTimer()
    sender_display = info.get("sender_display", "unknown")
    file_name = build_filename_from_message(msg, sender_display)

    chat_id = info.get("chat_id")
    msg_id = msg.id
    unique = uuid.uuid4().hex[:8]

    m=manager(spool_dir);allocated=[]
    def allocate():
        result=m.allocate(source_kind,chat_id,f"{msg_id}_{unique}_{file_name}");allocated.append(result);return result
    try:token,fd,save_path=await _io(allocate)
    except BaseException:
        if allocated:
            token,fd,_=allocated[0];os.close(fd);await _join(asyncio.create_task(_io(m.acquisition_failed,token)))
        raise
    stream=os.fdopen(fd,'w+b',buffering=0)
    extra={'chat_id':chat_id,'msg_id':msg_id,'source_kind':source_kind,'save_path':str(save_path)}
    bridge=None;identity=None;finished=False
    try:
        bridge=await configured_bridge();identity=message_identity(msg,chat_id) if bridge else None
        async def acquire():
            fpath=await bridge.materialize(identity,save_path,owned_stream=stream) if bridge else None
            if fpath is None:
                async with download_semaphore:
                    async def download():
                        stream.seek(0);stream.truncate()
                        return await msg.download_media(stream)
                    fpath=await run_api(download,op='snapshot_download_media',extra=extra)
            if fpath is None:raise RuntimeError('Empty owned acquisition')
            if fpath is not stream and Path(fpath)!=save_path:raise ValueError('Downloader returned an unowned spool path')
            stream.flush();await _io(os.fsync,stream.fileno())
        acquisition=asyncio.create_task(acquire())
        try:await asyncio.shield(acquisition)
        except asyncio.CancelledError:
            acquisition.cancel()
            try:await _join(acquisition)
            except BaseException:pass
            raise
        stream.close()
        shared_safe=bridge is None
        if bridge:
            published=await bridge.publish(identity,save_path)
            shared_safe=not bridge.persistent_cache_enabled or (Path(published)!=save_path and (Path(published).stat().st_dev,Path(published).stat().st_ino)!=(save_path.stat().st_dev,save_path.stat().st_ino))
        fpath=await _io(m.finalize,token,shared_safe);finished=True
    finally:
        if not stream.closed:stream.close()
        if not finished:await _join(asyncio.create_task(_io(m.acquisition_failed,token)))
    snapshot={"path":str(fpath),"media_type":media_type(msg),"media_size":get_media_size(msg),
              "caption_text":(msg.text or msg.message or '').strip(),"msg_id":msg.id,'spool_token':token}

    if identity:
        snapshot["shared_identity"] = identity

    log({
        "ts": tstamp(),
        "type": "perf",
        "step": "snapshot_media_message",
        "source_kind": source_kind,
        "chat_id": chat_id,
        "msg_id": msg_id,
        "ms": timer.ms(),
    })

    log({
        "ts": tstamp(),
        "type": "snapshot",
        "source_kind": source_kind,
        "chat_id": chat_id,
        "chat_title": info.get("chat_title"),
        "msg_id": msg_id,
        "sender_id": info.get("sender_id"),
        "sender_username": info.get("sender_username"),
        "sender_display": info.get("sender_display"),
        "media_type": snapshot["media_type"],
        "media_size": snapshot["media_size"],
        "file": str(fpath),
    })
    return snapshot


async def snapshot_album_messages(msgs: List[Any], info: dict, source_kind: str, spool_dir: Path, download_semaphore, run_api, log) -> List[dict]:
    sorted_msgs = sorted(msgs, key=lambda m: m.id)
    children=[asyncio.create_task(snapshot_media_message(m,info,source_kind,spool_dir,download_semaphore,run_api,log)) for m in sorted_msgs]
    gathering=asyncio.gather(*children,return_exceptions=True)
    try:snapshots=await asyncio.shield(gathering)
    except BaseException:
        for child in children:
            if not child.done():child.cancel()
        snapshots=await _join(gathering)
        tokens=[v['spool_token'] for v in snapshots if isinstance(v,dict) and v.get('spool_token')]
        if tokens:await _join(asyncio.create_task(_io(manager(spool_dir).retain_unpublished,tokens,'canceled-album')))
        raise

    result = []
    for item in snapshots:
        if isinstance(item, BaseException):
            log({
                "ts": tstamp(),
                "type": "err",
                "op": "snapshot_album_messages",
                "err": item.__class__.__name__,
                "msg": str(item),
                "chat_id": info.get("chat_id"),
                "grouped_id": info.get("grouped_id"),
            })
            continue
        if item:
            result.append(item)
    if len(result)!=len(sorted_msgs) and any(i.get('spool_token') for i in result):
        await _io(manager(spool_dir).retain_unpublished,[i['spool_token'] for i in result],'partial-album')
        raise RuntimeError('Partial owned album retained unpublished; not sent as complete')
    return result


__all__ = [
    "snapshot_album_messages",
    "snapshot_media_message",
]
