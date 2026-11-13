"""Standalone bot poller and read-only deployed source inspection CLI."""
import argparse
import asyncio
import fcntl
import json
import logging
import os
import time
from pathlib import Path
from dotenv import load_dotenv
from telegram import Bot, Update
from telegram.error import Conflict, RetryAfter
from telethon import TelegramClient
from telethon.sessions import StringSession
from .config import Config, positive, actor
from .filesystems import validate as validate_filesystems, FilesystemError
from .store import Store
from .engine import Engine, RateGate
from .source import Resolver, scopes
from .controller import Controller
from .input import parse_url
from .delivery import BotDelivery
from .inspection import summary, serve, inspect_running, health_running

LOG = logging.getLogger(__name__)


class StartupError(RuntimeError):
    pass


def environment():
    secret = os.getenv("CONTROL_ENV_FILE")
    if secret:
        try:
            with open(secret, encoding="utf-8") as f:
                load_dotenv(stream=f, override=False)
        except OSError:
            raise StartupError("CONTROL_ENV_FILE is unreadable") from None
    try:
        return Config.from_env()
    except (ValueError, TypeError, KeyError):
        raise StartupError("Invalid control configuration: check allowlist, numeric targets and positive limits") from None


def client_from_env():
    try:
        api_id = positive(os.environ["API_ID"], 2**31-1)
        api_hash = os.environ["API_HASH"]
        session = os.getenv("MEDIA_USER_SESSION_STRING")
        filename = os.getenv("MEDIA_USER_SESSION_FILE")
        if bool(session) == bool(filename) or not api_hash:
            raise ValueError()
        if filename:
            p = Path(filename).absolute()
            legacy = Path(os.getenv("SESSION_NAME", "my_account")).absolute()
            if not p.is_file() or p.is_symlink() or p == legacy or str(p) == str(legacy)+".session":
                raise ValueError()
        return TelegramClient(StringSession(session) if session else filename, api_id, api_hash,
                              receive_updates=False, flood_sleep_threshold=0, request_retries=0)
    except (ValueError, KeyError, TypeError):
        raise StartupError("Control requires API_ID/API_HASH and exactly one separate MEDIA_USER_SESSION_STRING or explicit MEDIA_USER_SESSION_FILE") from None


async def inbox_loop(controller, store):
    while True:
        row = store.db.execute("SELECT id,payload,attempt FROM updates WHERE state='pending' AND due<=? ORDER BY id LIMIT 1", (time.time(),)).fetchone()
        if row:
            store.db.execute("UPDATE updates SET state='processing',attempt=attempt+1 WHERE id=?", (row["id"],))
            try:
                await controller.handle(Update.de_json(json.loads(row["payload"]), controller.bot))
                store.db.execute("UPDATE updates SET state='done' WHERE id=?", (row["id"],))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                store.db.execute("UPDATE updates SET state=?,due=? WHERE id=?", ("pending" if row["attempt"]+1 < controller.config.retries else "failed", time.time()+5, row["id"]))
                LOG.warning("control update failed id=%s class=%s", row["id"], type(exc).__name__)
        else:
            await asyncio.sleep(0.25)


def ingest_authorized(update, store, allowed):
    callback = getattr(update, "callback_query", None)
    message = callback.message if callback else getattr(update, "message", None)
    try:
        if message is None or getattr(message, "sender_chat", None) is not None:
            raise PermissionError()
        actor(callback if callback else message, allowed)
    except PermissionError:
        # Offset commits without retaining any forbidden raw payload.
        with store.transaction():
            store.meta("offset", max(int(store.get_meta("offset")), update.update_id+1))
        return False
    store.ingest(update.to_dict())
    return True


async def polling(bot, store, allowed):
    while True:
        try:
            updates = await bot.get_updates(offset=int(store.get_meta("offset")), timeout=30,
                                            allowed_updates=["message", "callback_query"])
            for update in updates:
                ingest_authorized(update, store, allowed)
        except Conflict:
            raise RuntimeError("Another poller/webhook owns this bot; stop old downloader before starting control") from None
        except RetryAfter as exc:
            delay = exc.retry_after.total_seconds() if hasattr(exc.retry_after, "total_seconds") else exc.retry_after
            await asyncio.sleep(float(delay))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOG.warning("control polling delayed class=%s", type(exc).__name__)
            await asyncio.sleep(5)


async def run(inspect_url=None, health=False):
    os.umask(0o077)
    config = environment()
    validate_filesystems(config)
    if health:
        print(json.dumps(await health_running(config)))
        return
    if inspect_url:
        result = await inspect_running(config, inspect_url)
        if result is not None:
            if "error" in result:
                raise StartupError("Inspection failed: "+result["error"])
            print(json.dumps(result, ensure_ascii=False))
            return
    config.db.parent.mkdir(parents=True, exist_ok=True)
    with open(str(config.db)+".lock", "a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Control state already owned by another process") from None
        store = Store(config.db)
        client = client_from_env()
        try:
            await client.connect()
            if not await client.is_user_authorized():
                raise RuntimeError("Downloader user session not authorized; no interactive login attempted")
            from tg_forwarder.shared_media import configured_bridge
            bridge = await configured_bridge()
            gate = RateGate(store, bridge)
            resolver = Resolver(client, config.scan_limit, gate)
            if inspect_url:
                snap = await resolver.resolve(parse_url(inspect_url))
                print(json.dumps(summary(snap), ensure_ascii=False))
                return
            token = os.getenv("TELEGRAM_BOT_TOKEN")
            if not token:
                raise RuntimeError("TELEGRAM_BOT_TOKEN required in APP_MODE=control")
            logging.getLogger("httpx").setLevel(logging.ERROR)
            logging.getLogger("telethon").setLevel(logging.ERROR)
            async with Bot(token) as bot:
                bot_id = (await bot.get_me()).id
                prior_bot = store.get_meta("bot_principal", "")
                if prior_bot and prior_bot != str(bot_id):
                    raise StartupError("Control state belongs to a different bot")
                store.meta("bot_principal", bot_id)
                engine = Engine(config, store, client, gate, delivery=BotDelivery(bot, positive(os.getenv("CONTROL_BOT_UPLOAD_MAX_BYTES", str(49*1024*1024)))), bridge=bridge, bot_id=bot_id)
                controller = Controller(config, store, resolver, engine, bot)
                async def notify(task):
                    if task["reply"] or task["state"] in ("done", "failed", "ambiguous"):
                        await controller.render_task(task["owner"], task["chat"], task["reply"] or None, task)
                engine.notify = notify
                store.recover()
                tasks = [asyncio.create_task(polling(bot, store, config.allowed)), asyncio.create_task(inbox_loop(controller, store)),
                         asyncio.create_task(controller.watch_loop()), asyncio.create_task(controller.preview_loop()), asyncio.create_task(serve(config, resolver))]
                tasks += [asyncio.create_task(engine.worker()) for _ in range(config.concurrency)]
                try:
                    await asyncio.gather(*tasks)
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await client.disconnect()
            store.close()


def cli():
    parser = argparse.ArgumentParser(description="Telegram media workbench")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--health", action="store_true", help="Local socket health; no Telegram calls or second auth client")
    mode.add_argument("--inspect", metavar="URL", help="Read-only snapshot/counts; no bot poll, download or forward")
    args = parser.parse_args()
    try:
        asyncio.run(run(args.inspect,health=args.health))
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        print("Control stopped: "+(str(exc) if isinstance(exc, (StartupError, FilesystemError)) else type(exc).__name__+" (check configuration/access; credentials not logged)"), flush=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    cli()
