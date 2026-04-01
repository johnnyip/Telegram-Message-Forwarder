import asyncio
from dataclasses import dataclass
import shutil
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional

from telethon import errors

from ..storage.edit_mapping import store_album_mapping, store_message_mapping
from ..core.utils import tstamp


RETAIN_DIRNAME = "retained"


def _retain_path(original: Path) -> Path:
    parts = list(original.parts)
    if "_spool" in parts:
        idx = parts.index("_spool")
        retained_parts = parts[:idx] + [RETAIN_DIRNAME] + parts[idx + 1:]
        return Path(*retained_parts)
    return original.parent / RETAIN_DIRNAME / original.name


def retain_file(path: Path, ctx, *, info: dict, kind: str) -> Path:
    target = _retain_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if path.resolve() == target.resolve():
        return path
    shutil.move(str(path), str(target))
    ctx.log({
        "ts": tstamp(),
        "type": "retain",
        "op": "retain_local_file",
        "kind": kind,
        "chat_id": info.get("chat_id"),
        "msg_id": info.get("msg_id"),
        "grouped_id": info.get("grouped_id"),
        "from_path": str(path),
        "to_path": str(target),
    })
    return target


@dataclass
class JobProcessingContext:
    app_mode: str
    bot: Any
    client: Any
    route_map: Dict[str, Dict[str, Any]]
    ignore_users: set
    ignore_ids: set
    delete_after_send: bool
    text_target_parallel: bool
    media_target_parallel: bool
    log: Callable[[dict], None]
    should_ignore: Callable[[dict, set, set], bool]
    build_header_from_info: Callable[[dict, bool], str]
    is_stale_file: Callable[[str], bool]
    send_text_to_target: Callable[[Dict[str, Any], Any, str, dict, str], Awaitable[bool]]
    send_file_to_target: Callable[[Dict[str, Any], Any, Path, str, dict, str], Awaitable[bool]]
    send_album_to_target: Callable[[Dict[str, Any], Any, List[str], str, dict, str], Awaitable[bool]]
    fetch_message_by_id: Callable[[int, int], Awaitable[Any]]
    fetch_messages_by_ids: Callable[[int, List[int]], Awaitable[List[Any]]]
    run_api: Callable[..., Awaitable[Any]]
    snapshot_media_message: Callable[[Any, dict, str], Awaitable[Optional[dict]]]
    snapshot_album_messages: Callable[[List[Any], dict, str], Awaitable[List[dict]]]
    forward_cache_set: Callable[[int, bool], None]


def resolve_routes(route_map: Dict[str, Dict[str, Any]], route_names: List[str]) -> List[Dict[str, Any]]:
    return [route_map[name] for name in route_names if name in route_map]


async def _run_target_coroutines(coroutines: List[Awaitable[bool]], *, parallel: bool) -> List[bool]:
    if parallel:
        return await asyncio.gather(*coroutines)
    results: List[bool] = []
    for coroutine in coroutines:
        results.append(await coroutine)
    return results


async def process_text_job(job: dict, ctx: JobProcessingContext) -> List[str]:
    info = job["info"]
    if ctx.should_ignore(info, ctx.ignore_users, ctx.ignore_ids):
        ctx.log({"ts": tstamp(), "type": "info", "note": "ignored_text_job", "chat_id": info["chat_id"], "msg_id": info["msg_id"]})
        return []

    is_edit = job["source_kind"] == "edited"
    hdr = ctx.build_header_from_info(info, is_edit=is_edit)
    combined = f"{hdr}\n{job['text']}"
    routes = resolve_routes(ctx.route_map, job.get("route_names", []))

    success_count = 0
    total_count = 0
    for route in routes:
        coroutines = [ctx.send_text_to_target(route, target, combined, info, job["source_kind"]) for target in route["targets"]]
        results = await _run_target_coroutines(coroutines, parallel=ctx.text_target_parallel)
        total_count += len(results)
        success_count += sum(1 for ok in results if ok)
        for target, result in zip(route["targets"], results):
            if result and getattr(result, "sent_message_id", None):
                await store_message_mapping(info["chat_id"], info["msg_id"], target, {
                    "message_id": result.sent_message_id,
                    "delivery_kind": getattr(result, "delivery_kind", None),
                    "route": route["name"],
                })

    if total_count > 0 and success_count == 0:
        raise RuntimeError(f"text job failed for all targets chat_id={info['chat_id']} msg_id={info['msg_id']}")
    return []


async def process_media_file_job(job: dict, ctx: JobProcessingContext) -> List[str]:
    info = job["info"]
    if ctx.should_ignore(info, ctx.ignore_users, ctx.ignore_ids):
        ctx.log({"ts": tstamp(), "type": "info", "note": "ignored_media_file_job", "chat_id": info["chat_id"], "msg_id": info["msg_id"]})
        return []

    snapshot = job["snapshot"]
    info["_media_type"] = snapshot.get("media_type")
    fpath = Path(snapshot["path"])
    if not fpath.exists():
        ctx.log({"ts": tstamp(), "type": "err", "op": "media_file_missing", "chat_id": info["chat_id"], "msg_id": info["msg_id"], "file": str(fpath)})
        return []
    if ctx.is_stale_file(str(fpath)):
        ctx.log({"ts": tstamp(), "type": "warn", "op": "media_file_stale", "chat_id": info["chat_id"], "msg_id": info["msg_id"], "file": str(fpath)})

    is_edit = job["source_kind"] == "edited"
    hdr = ctx.build_header_from_info(info, is_edit=is_edit)
    caption = f"{hdr}\n{snapshot.get('caption_text', '').strip()}".strip()
    routes = resolve_routes(ctx.route_map, job["route_names"])

    success_count = 0
    total_count = 0
    preserve_local_copy = False
    current_path = fpath
    for route in routes:
        coroutines = [ctx.send_file_to_target(route, target, current_path, caption, info, job["source_kind"]) for target in route["targets"]]
        results = await _run_target_coroutines(coroutines, parallel=ctx.media_target_parallel)
        total_count += len(results)
        success_count += sum(1 for ok in results if ok)
        for target, result in zip(route["targets"], results):
            if result and getattr(result, "sent_message_id", None):
                await store_message_mapping(info["chat_id"], info["msg_id"], target, {
                    "message_id": result.sent_message_id,
                    "delivery_kind": getattr(result, "delivery_kind", None),
                    "route": route["name"],
                })
        route_wants_preserve = any(getattr(result, "preserve_local_copy", False) for result in results)
        if route_wants_preserve and not preserve_local_copy:
            current_path = retain_file(current_path, ctx, info=info, kind="media_file")
            job["snapshot"]["path"] = str(current_path)
            preserve_local_copy = True
        else:
            preserve_local_copy = preserve_local_copy or route_wants_preserve

    if total_count > 0 and success_count == 0:
        raise RuntimeError(f"media_file job failed for all targets chat_id={info['chat_id']} msg_id={info['msg_id']}")
    return [] if preserve_local_copy else ([str(current_path)] if ctx.delete_after_send else [])


async def process_media_album_file_job(job: dict, ctx: JobProcessingContext) -> List[str]:
    info = job["info"]
    if ctx.should_ignore(info, ctx.ignore_users, ctx.ignore_ids):
        ctx.log({"ts": tstamp(), "type": "info", "note": "ignored_media_album_file_job", "chat_id": info["chat_id"], "msg_id": info["msg_id"]})
        return []

    snapshots = job["snapshots"]
    files = [snapshot["path"] for snapshot in snapshots if Path(snapshot["path"]).exists()]
    info["_album_snapshots"] = snapshots
    if not files:
        ctx.log({"ts": tstamp(), "type": "err", "op": "media_album_files_missing", "chat_id": info["chat_id"], "grouped_id": info.get("grouped_id")})
        return []

    stale_files = [path for path in files if ctx.is_stale_file(path)]
    if stale_files:
        ctx.log({"ts": tstamp(), "type": "warn", "op": "media_album_files_stale", "chat_id": info["chat_id"], "grouped_id": info.get("grouped_id"), "count": len(stale_files)})

    is_edit = job["source_kind"] == "edited"
    hdr = ctx.build_header_from_info(info, is_edit=is_edit)
    caption_body = snapshots[0].get("caption_text", "").strip() if snapshots else ""
    caption = f"{hdr}\n{caption_body}".strip()
    routes = resolve_routes(ctx.route_map, job["route_names"])

    success_count = 0
    total_count = 0
    preserve_local_copy = False
    current_files = files[:]
    for route in routes:
        coroutines = [ctx.send_album_to_target(route, target, current_files, caption, info, job["source_kind"]) for target in route["targets"]]
        results = await _run_target_coroutines(coroutines, parallel=ctx.media_target_parallel)
        total_count += len(results)
        success_count += sum(1 for ok in results if ok)
        for target, result in zip(route["targets"], results):
            if result and getattr(result, "sent_message_id", None):
                await store_album_mapping(info["chat_id"], info.get("grouped_id"), target, {
                    "message_id": result.sent_message_id,
                    "message_ids": getattr(result, "sent_message_ids", None),
                    "delivery_kind": getattr(result, "delivery_kind", None),
                    "route": route["name"],
                })
        route_wants_preserve = any(getattr(result, "preserve_local_copy", False) for result in results)
        if route_wants_preserve and not preserve_local_copy:
            retained_files = []
            for idx, original in enumerate(current_files):
                retained = retain_file(Path(original), ctx, info=info, kind="media_album_file")
                retained_files.append(str(retained))
                if idx < len(job["snapshots"]):
                    job["snapshots"][idx]["path"] = str(retained)
            current_files = retained_files
            preserve_local_copy = True
        else:
            preserve_local_copy = preserve_local_copy or route_wants_preserve

    if total_count > 0 and success_count == 0:
        raise RuntimeError(f"media_album_file job failed for all targets chat_id={info['chat_id']} grouped_id={info.get('grouped_id')}")
    return [] if preserve_local_copy else (current_files if ctx.delete_after_send else [])


async def process_media_forward_job(job: dict, ctx: JobProcessingContext) -> List[str]:
    info = job["info"]
    if ctx.should_ignore(info, ctx.ignore_users, ctx.ignore_ids):
        ctx.log({"ts": tstamp(), "type": "info", "note": "ignored_media_forward_job", "chat_id": info["chat_id"], "msg_id": info["msg_id"]})
        return []

    if ctx.app_mode == "send" and ctx.bot is not None:
        ctx.log({"ts": tstamp(), "type": "warn", "op": "media_forward_job_unsupported_in_bot_mode", "chat_id": info["chat_id"], "msg_id": info["msg_id"]})
        return []

    msg = await ctx.fetch_message_by_id(job["chat_id"], job["msg_id"])
    if not msg:
        ctx.log({"ts": tstamp(), "type": "err", "op": "media_forward_fetch_failed", "chat_id": job["chat_id"], "msg_id": job["msg_id"]})
        return []

    is_edit = job["source_kind"] == "edited"
    hdr = ctx.build_header_from_info(info, is_edit=is_edit)
    routes = resolve_routes(ctx.route_map, job["route_names"])

    success_count = 0
    total_count = 0
    try:
        for route in routes:
            for target in route["targets"]:
                total_count += 1
                extra = {
                    "route": route["name"],
                    "dst": target,
                    "src_msg": info["msg_id"],
                    "chat_id": info["chat_id"],
                    "chat_title": info["chat_title"],
                    "sender_id": info["sender_id"],
                    "sender_username": info["sender_username"],
                    "sender_display": info["sender_display"],
                    "source_kind": job["source_kind"],
                    "media_size": job.get("media_size"),
                    "media_type": job.get("media_type"),
                }
                await ctx.run_api(ctx.client.send_message(target, hdr), op="send_header_before_forward_large", extra=extra)
                await ctx.run_api(ctx.client.forward_messages(target, msg, msg.peer_id), op="forward_large_media", extra=extra)
                ctx.log({"ts": tstamp(), "type": "out", "op": "forward_large_media", "status": "ok", **extra})
                success_count += 1

        if success_count > 0:
            ctx.forward_cache_set(info["chat_id"], True)
        if total_count > 0 and success_count == 0:
            raise RuntimeError(f"media_forward job failed for all targets chat_id={info['chat_id']} msg_id={info['msg_id']}")
        return []
    except errors.ChatForwardsRestrictedError:
        ctx.forward_cache_set(info["chat_id"], False)
        ctx.log({"ts": tstamp(), "type": "warn", "op": "forward_large_media", "note": "chat_forwards_restricted_fallback_to_download", "chat_id": info["chat_id"], "msg_id": info["msg_id"]})
        snapshot = await ctx.snapshot_media_message(msg, info, job["source_kind"])
        if not snapshot:
            return []
        fallback_job = {"job_type": "media_file", "source_kind": job["source_kind"], "route_names": job["route_names"], "info": info, "snapshot": snapshot}
        return await process_media_file_job(fallback_job, ctx)


async def process_media_album_forward_job(job: dict, ctx: JobProcessingContext) -> List[str]:
    info = job["info"]
    if ctx.should_ignore(info, ctx.ignore_users, ctx.ignore_ids):
        ctx.log({"ts": tstamp(), "type": "info", "note": "ignored_media_album_forward_job", "chat_id": info["chat_id"], "msg_id": info["msg_id"]})
        return []

    if ctx.app_mode == "send" and ctx.bot is not None:
        ctx.log({"ts": tstamp(), "type": "warn", "op": "media_album_forward_job_unsupported_in_bot_mode", "chat_id": info["chat_id"], "grouped_id": info.get("grouped_id")})
        return []

    msgs = await ctx.fetch_messages_by_ids(job["chat_id"], job["msg_ids"])
    msgs = sorted([msg for msg in msgs if msg], key=lambda msg: msg.id)
    if not msgs:
        ctx.log({"ts": tstamp(), "type": "err", "op": "media_album_forward_fetch_failed", "chat_id": job["chat_id"], "msg_ids": job["msg_ids"]})
        return []

    is_edit = job["source_kind"] == "edited"
    hdr = ctx.build_header_from_info(info, is_edit=is_edit)
    routes = resolve_routes(ctx.route_map, job["route_names"])

    success_count = 0
    total_count = 0
    try:
        for route in routes:
            for target in route["targets"]:
                total_count += 1
                extra = {
                    "route": route["name"],
                    "dst": target,
                    "chat_id": info["chat_id"],
                    "chat_title": info["chat_title"],
                    "sender_id": info["sender_id"],
                    "sender_username": info["sender_username"],
                    "sender_display": info["sender_display"],
                    "source_kind": job["source_kind"],
                    "msg_ids": job["msg_ids"],
                    "grouped_id": info.get("grouped_id"),
                }
                await ctx.run_api(ctx.client.send_message(target, hdr), op="send_header_before_forward_album", extra=extra)
                await ctx.run_api(ctx.client.forward_messages(target, msgs, msgs[0].peer_id), op="forward_large_album", extra=extra)
                ctx.log({"ts": tstamp(), "type": "out", "op": "forward_large_album", "status": "ok", **extra})
                success_count += 1

        if success_count > 0:
            ctx.forward_cache_set(info["chat_id"], True)
        if total_count > 0 and success_count == 0:
            raise RuntimeError(f"media_album_forward job failed for all targets chat_id={info['chat_id']} grouped_id={info.get('grouped_id')}")
        return []
    except errors.ChatForwardsRestrictedError:
        ctx.forward_cache_set(info["chat_id"], False)
        ctx.log({"ts": tstamp(), "type": "warn", "op": "forward_large_album", "note": "chat_forwards_restricted_fallback_to_download", "chat_id": info["chat_id"], "grouped_id": info.get("grouped_id"), "msg_ids": job["msg_ids"]})
        snapshots = await ctx.snapshot_album_messages(msgs, info, job["source_kind"])
        if not snapshots:
            return []
        fallback_job = {"job_type": "media_album_file", "source_kind": job["source_kind"], "route_names": job["route_names"], "info": info, "snapshots": snapshots}
        return await process_media_album_file_job(fallback_job, ctx)


async def dispatch_media_job(job: dict, ctx: JobProcessingContext) -> List[str]:
    job_type = job.get("job_type")
    if job_type == "media_file":
        return await process_media_file_job(job, ctx)
    if job_type == "media_forward":
        return await process_media_forward_job(job, ctx)
    if job_type == "media_album_file":
        return await process_media_album_file_job(job, ctx)
    if job_type == "media_album_forward":
        return await process_media_album_forward_job(job, ctx)
    raise RuntimeError(f"unknown media job_type={job_type}")


__all__ = [
    "JobProcessingContext",
    "RETAIN_DIRNAME",
    "dispatch_media_job",
    "process_media_album_file_job",
    "process_media_album_forward_job",
    "process_media_file_job",
    "process_media_forward_job",
    "process_text_job",
    "resolve_routes",
    "retain_file",
]
