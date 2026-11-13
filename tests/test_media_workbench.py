"""Offline fake-client integration contracts; socket connections are forbidden."""
import asyncio
from dataclasses import replace
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import socket
import tempfile
import time
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock, patch
from telegram import Update
from telegram.error import NetworkError, RetryAfter
from telethon import functions
from telethon.errors import FloodWaitError
from tg_forwarder.control.config import Config, actor, allowed_ids
from tg_forwarder.control.input import parse_url, from_message, SourceRef
from tg_forwarder.control.source import Resolver, scopes, item
from tg_forwarder.control.store import Store, digest
from tg_forwarder.control.engine import Engine, RateGate, Archive, Ambiguous
from tg_forwarder.control.controller import Controller
from tg_forwarder.control.delivery import BotDelivery
from tg_forwarder.control.runtime import environment, inbox_loop, polling

CHANNEL = -1004433596337
GROUP = -1003837422582
OWNER = 192114587


def message(chat, mid, album=None, kind="photo", source=None):
    m = NS(chat_id=chat, id=mid, grouped_id=album, photo=True if kind=="photo" else None,
           video=True if kind=="video" else None, date=datetime(2026,11,13,tzinfo=timezone.utc),
           file=NS(size=4), message="caption "+str(mid), noforwards=False,
           media=True if kind else None, action=None, peer_id=chat, fwd_from=source)
    async def download(file, progress_callback):
        await progress_callback(0,4)
        file.write(b"data")
        await progress_callback(4,4)
        return file
    m.download_media = AsyncMock(side_effect=download)
    return m


class FakeClient:
    def __init__(self):
        self.channel = NS(id=4433596337, key=CHANNEL, noforwards=False)
        self.group = NS(id=3837422582, key=GROUP, noforwards=False)
        self.main = [message(CHANNEL, mid, 1) for mid in range(4833,4843)]
        self.roots = [message(GROUP, mid, 2, source=NS(channel_post=4833+i, from_id=NS(channel_id=4433596337))) for i,mid in enumerate(range(24124,24134))]
        self.replies=[]
        for a in range(6):
            for n in range(10):
                # 49 photos +11 videos, last album 6 photos +4 videos.
                video = (n >= (7 if a==0 else 9)) if a<5 else n>=6
                self.replies.append(message(GROUP,24145+a*10+n,101+a,"video" if video else "photo"))
        self.replies += [message(GROUP,24205,kind=None), message(GROUP,24206,kind=None)]
        self.replies[-1].reply_to = NS(reply_to_msg_id=24150, reply_to_top_id=24124)
        self.calls=[]
        self.page_size=17
        self.denied=False
        self.partial=False

    async def get_entity(self, peer):
        key = getattr(peer,"key", peer)
        if hasattr(peer, "channel_id"):
            key = -1000000000000-peer.channel_id
        return self.channel if key in (CHANNEL,"tgfanchabiao") else self.group

    async def get_messages(self, peer, ids):
        entity=await self.get_entity(peer)
        values=self.main if entity.key==CHANNEL else self.roots+self.replies
        return next((m for m in values if m.id==ids), None)

    def iter_messages(self, peer, limit, **kwargs):
        async def iterator():
            entity=await self.get_entity(peer)
            values=self.main if entity.key==CHANNEL else self.roots+self.replies
            if kwargs.get("reverse"):
                values=sorted([m for m in values if m.id>kwargs["min_id"]],key=lambda m:m.id)
            else:
                values=sorted([m for m in values if m.id<kwargs["offset_id"]],key=lambda m:m.id,reverse=True)
            for m in values[:limit]:
                yield m
        return iterator()

    async def __call__(self, req):
        self.calls.append(req)
        if self.denied:
            raise PermissionError("private")
        if isinstance(req,functions.messages.GetDiscussionMessageRequest):
            return NS(messages=[self.roots[-1]])
        assert isinstance(req,functions.messages.GetRepliesRequest)
        assert req.msg_id==24124, "must use FIRST root album member"
        values=sorted(self.roots+self.replies,key=lambda m:m.id,reverse=True)
        total=len(values)
        if req.min_id:
            values=[m for m in values if m.id>req.min_id]
        if req.max_id:
            values=[m for m in values if m.id<req.max_id]
        if req.offset_id:
            values=[m for m in values if m.id<req.offset_id]
        if self.partial and req.offset_id:
            values=[]
        return NS(messages=values[:min(self.page_size,req.limit)],count=total)


class Base(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.net=patch.object(socket.socket,"connect",side_effect=AssertionError("network forbidden"))
        self.net.start();self.addCleanup(self.net.stop)
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.config=Config(frozenset({OWNER}),((OWNER,None),(-1002,7)),self.root/"archive",self.root/"state.db",reserve_bytes=0)
        self.store=Store(self.config.db);self.addCleanup(self.store.close)
        self.client=FakeClient()
        self.peer=patch("tg_forwarder.control.source.utils.get_peer_id",side_effect=lambda x:x.key)
        self.peer.start();self.addCleanup(self.peer.stop)
        self.gate=RateGate(self.store)
        self.resolver=Resolver(self.client,gate=self.gate)
        self.delivery=NS(send=AsyncMock(side_effect=lambda t,g,f:[NS(message_id=900+n) for n in range(len(g))]))
        self.engine=Engine(self.config,self.store,self.client,self.gate,delivery=self.delivery)
        self.bot=NS(send_message=AsyncMock(return_value=NS(message_id=100)),edit_message_text=AsyncMock(),answer_callback_query=AsyncMock())
        self.controller=Controller(self.config,self.store,self.resolver,self.engine,self.bot)

    async def snapshot(self, comment=None):
        return await self.resolver.resolve(SourceRef("tgfanchabiao",4833,comment))

    def update(self,text="/tasks",uid=OWNER,**extra):
        msg={"message_id":1,"date":1,"chat":{"id":OWNER,"type":"private"},
             "from":{"id":uid,"is_bot":False,"first_name":"owner"},"text":text,**extra}
        return Update.de_json({"update_id":42,"message":msg},None)

    async def payload(self,action="download",target=None,count=1):
        snap=await self.snapshot()
        return {"snapshot":snap,"items":snap["main"][:count],"action":action,"target":target}

    async def run_task(self,payload):
        task=self.store.create_task(OWNER,OWNER,100,payload)
        if task["state"] in ("queued","delayed"):
            claimed=self.store.claim()
            await self.engine.execute(claimed)
        return self.store.task(task["id"],OWNER,OWNER)


class SourceTests(Base):
    async def test_inspected_fixture_70_media_and_last_root_dedupe(self):
        snap=await self.snapshot()
        self.assertFalse(snap["partial"])
        self.assertEqual((snap["thread"]["chat"],snap["thread"]["id"]),(GROUP,24124))
        self.assertEqual(len(snap["main"]),10)
        self.assertEqual(len(snap["discussion"]),62)
        all_media=scopes(snap)["all"]
        self.assertEqual(len(all_media),70)
        self.assertEqual(sum(i["kind"]=="photo" for i in all_media),59)
        self.assertEqual(sum(i["kind"]=="video" for i in all_media),11)
        self.assertEqual([i["id"] for i in snap["main"]],list(range(4833,4843)))
        self.assertGreater(len(self.client.calls),4)
        self.assertTrue(any(i["id"]==24206 for i in snap["discussion"]))

    async def test_comment_points_to_last_six_photo_four_video_album(self):
        snap=await self.snapshot(24204)
        self.assertEqual(snap["selected"][0]["id"],24204)
        self.assertEqual(len(snap["selected_album"]),10)
        self.assertEqual(sum(i["kind"]=="video" for i in snap["selected_album"]),4)
        self.assertEqual(len(scopes(snap)["discussion"]),60)
        self.assertEqual(len(scopes(snap)["all"]),70)

    async def test_album_member_alias_normalizes(self):
        one=await self.snapshot()
        two=await self.resolver.resolve(SourceRef(CHANNEL,4842))
        self.assertEqual(one["post"],two["post"])
        self.assertEqual(one["main"],two["main"])
        self.assertEqual(two["selected"][0]["id"],4842)

    async def test_sparse_album_ids_not_plus_minus_ten(self):
        self.client.main=[message(CHANNEL,4800+i*20,1) for i in range(10)]
        snap=await self.resolver.resolve(SourceRef(CHANNEL,4880))
        self.assertEqual(len(snap["main"]),10)

    async def test_access_denied_is_partial_not_zero_success(self):
        self.client.denied=True
        snap=await self.snapshot()
        self.assertTrue(snap["partial"])
        self.assertIn("不可用",snap["warning"])

    async def test_partial_pagination_visible(self):
        self.client.partial=True
        snap=await self.snapshot()
        self.assertTrue(snap["partial"])
        self.assertIn("不完整",snap["warning"])

    async def test_comment_deleted_fails_explicitly(self):
        with self.assertRaises(RuntimeError):
            await self.snapshot(99999)

    async def test_scan_limit_partial(self):
        self.resolver.limit=10
        self.assertTrue((await self.snapshot())["partial"])

    async def test_distinct_nonempty_scopes_and_filter(self):
        snap=await self.snapshot()
        self.assertNotIn("single",scopes(snap,"video"))
        self.assertNotIn("album",scopes(snap,"video"))
        snap["discussion"]=[]
        self.assertEqual(list(scopes(snap)),["single","album"])

    async def test_channel_protection_propagates_to_single(self):
        self.client.channel.noforwards=True
        snap=await self.snapshot()
        self.assertTrue(snap["selected"][0]["protected"])
        self.assertTrue(all(i["protected"] for i in snap["main"]))


class SecurityTests(Base):
    def test_allowlist_strict_and_empty_fail_closed(self):
        self.assertEqual(allowed_ids(""),frozenset())
        for raw in ("1,","0","-1","foo","1,2a","1,,2","1.0","+1","01","99999999999999999999999"):
            with self.subTest(raw=raw),self.assertRaises(ValueError):allowed_ids(raw)

    def test_url_allowlist_and_bounds(self):
        self.assertEqual(parse_url("https://t.me/c/4433596337/4833?comment=24204").peer,CHANNEL)
        self.assertEqual(parse_url("https://t.me/TGFanChabiao/4833?single").post,4833)
        for raw in ("http://t.me/abcd/1","https://evil/t.me/abcd/1","https://t.me@evil/abcd/1","https://t.me/abcd/0","https://t.me/abcd/2147483648","https://t.me/abcd/1?x=2","https://t.me/abcd/1?comment=0","https://t.me/abcd/1#x","https://t.me/c/1/2/3","https://t.me/abcd/1?comment=2&comment=3","https://t.me/abcd/1?comment=" ):
            with self.subTest(raw=raw),self.assertRaises(ValueError):parse_url(raw)
        with self.assertRaises(ValueError):parse_url("a"*513)

    def test_forwarded_metadata_and_hidden_source(self):
        msg=NS(text=None,caption=None,forward_origin=NS(chat=NS(id=CHANNEL),message_id=4833))
        self.assertEqual(from_message(msg),SourceRef(CHANNEL,4833))
        with self.assertRaises(ValueError):from_message(NS(text="",forward_origin=NS(sender_user_name="hidden")))

    async def test_all_commands_denied_before_access(self):
        for command in ("/tasks","/task aaa","/cancel aaa","/retry aaa","/resume aaa","/watches","/unwatch aaa","/health","https://t.me/TGFanChabiao/4833"):
            await self.controller.handle(self.update(command,uid=88))
        self.bot.send_message.assert_not_awaited()
        self.assertEqual(self.client.calls,[])

    async def test_sender_chat_and_bots_denied(self):
        await self.controller.handle(self.update("/tasks",sender_chat={"id":-1002,"type":"channel","title":"spoof"}))
        await self.controller.handle(self.update("/tasks",**{"from":{"id":OWNER,"is_bot":True,"first_name":"bot"}}))
        self.bot.send_message.assert_not_awaited()

    def test_capability_owner_chat_message_expiry_and_forgery(self):
        token=self.store.capability(OWNER,OWNER,100,{"x":1})
        self.assertEqual(self.store.use(token,OWNER,OWNER,100),{"x":1})
        for args in (("forged",OWNER,OWNER,100),(token,88,OWNER,100),(token,OWNER,88,100),(token,OWNER,OWNER,101)):
            with self.assertRaises(PermissionError):self.store.use(*args)
        self.store.db.execute("UPDATE callbacks SET expires=0")
        with self.assertRaises(PermissionError):self.store.use(token,OWNER,OWNER,100)

    def test_tasks_owner_and_chat_scoped(self):
        t=self.store.create_task(OWNER,OWNER,100,{"items":[{"chat":1,"id":1}],"action":"download"})
        self.assertEqual(self.store.tasks(88,OWNER),[])
        for owner,chat in ((88,OWNER),(OWNER,88)):
            with self.assertRaises(PermissionError):self.store.task(t["id"],owner,chat)

    def test_config_targets_and_env_aliases(self):
        with patch.dict(os.environ,{"CONTROL_ALLOWED_USER_IDS":str(OWNER),"CONTROL_TARGETS_JSON":'[ {"chat_id":192114587} ]',"CONTROL_STATE_DIR":str(self.root/"state"),"MEDIA_ARCHIVE_DIR":str(self.root/"media")},clear=True):
            c=Config.from_env()
            self.assertEqual(c.targets,((OWNER,None),))
            self.assertEqual(c.db,self.root/"state"/"control.sqlite3")
            self.assertEqual(c.root,self.root/"media")
        for raw in ('[{"chat_id":"123"}]','[{"chat_id":true}]','[{"chat_id":1,"thread_id":0}]','{}'):
            with patch.dict(os.environ,{"CONTROL_TARGETS_JSON":raw},clear=True),self.assertRaises(ValueError):Config.from_env()

    def test_secret_file_is_opt_in_and_unreadable_fails(self):
        with patch.dict(os.environ,{"CONTROL_ENV_FILE":str(self.root/"missing")},clear=True),self.assertRaisesRegex(RuntimeError,"unreadable"):
            environment()

    async def test_authenticated_health_actual_handler(self):
        await self.controller.handle(self.update("/health"))
        self.assertIn("已獲授權",self.bot.send_message.call_args.kwargs["text"])
        self.assertEqual(self.client.calls,[])

    async def test_authorized_hidden_forward_requests_url(self):
        await self.controller.handle(self.update("",forward_origin={"type":"hidden_user","date":1,"sender_user_name":"hidden"}))
        self.assertIn("原帖連結",self.bot.send_message.call_args.kwargs["text"])
        self.assertEqual(self.client.calls,[])


class TaskTests(Base):
    async def test_download_archive_manifest_and_repeated_claim(self):
        p=await self.payload(count=10)
        first=self.store.create_task(OWNER,OWNER,100,p)
        second=self.store.create_task(OWNER,OWNER,100,p)
        self.assertEqual(first["id"],second["id"])
        task=self.store.claim()
        self.assertIsNone(self.store.claim())
        await self.engine.execute(task)
        self.assertEqual(self.store.task(task["id"],OWNER,OWNER)["state"],"done")
        self.assertEqual(len(list(self.config.root.rglob("*.jpg"))),10)
        self.assertEqual(self.delivery.send.await_count,0)
        manifest=json.loads(next(self.config.root.rglob("manifest_*.json")).read_text())
        self.assertEqual(manifest["state"],"done")
        self.assertEqual(len(manifest["items"]),10)
        self.assertTrue(all(i["download"]=="done" for i in manifest["items"]))
        self.assertFalse(any(i.get("forward")=="done" for i in manifest["items"]))

    async def test_download_is_not_forward_and_target_is_distinct(self):
        p=await self.payload()
        self.assertEqual((await self.run_task(p))["state"],"done")
        self.assertEqual(self.delivery.send.await_count,0)
        p={**p,"action":"forward","target":[OWNER,None]}
        self.assertEqual((await self.run_task(p))["state"],"done")
        self.assertEqual(self.delivery.send.await_count,1)
        p={**p,"target":[-1002,7]}
        self.assertEqual((await self.run_task(p))["state"],"done")
        self.assertEqual(self.delivery.send.await_count,2)
        self.client.main[0].download_media.assert_awaited_once()

    async def test_overlapping_scope_forward_not_duplicated(self):
        p=await self.payload("forward",[OWNER,None],1)
        await self.run_task(p)
        p["items"]=p["snapshot"]["main"]
        await self.run_task(p)
        groups=[c.args[1] for c in self.delivery.send.await_args_list]
        self.assertEqual([len(g) for g in groups],[1,9])
        self.assertEqual(sum(i["id"]==4833 for g in groups for i in g),1)

    async def test_protected_download_allowed_but_forward_rejected(self):
        self.client.channel.noforwards=True
        p=await self.payload("forward",[OWNER,None])
        self.assertEqual((await self.run_task(p))["state"],"failed")
        self.delivery.send.assert_not_awaited()
        p["action"]="download"
        self.assertEqual((await self.run_task(p))["state"],"done")

    async def test_arbitrary_target_rejected(self):
        self.assertEqual((await self.run_task(await self.payload("forward",[999,None])))["state"],"failed")
        self.delivery.send.assert_not_awaited()

    async def test_cancel_then_resume_reuses_downloads(self):
        p=await self.payload(count=2)
        task=self.store.create_task(OWNER,OWNER,100,p)
        claimed=self.store.claim()
        self.store.set_task(task["id"],state="cancelled")
        await self.engine.execute(claimed)
        self.assertEqual(self.store.task(task["id"],OWNER,OWNER)["state"],"cancelled")
        await self.controller.task_command(OWNER,OWNER,"/resume",[task["id"]])
        await self.engine.execute(self.store.claim())
        self.assertEqual(self.store.task(task["id"],OWNER,OWNER)["state"],"done")

    async def test_retry_failed_preserves_successful_item_cache(self):
        p=await self.payload(count=2)
        m=self.client.main[1]
        original=m.download_media.side_effect
        m.download_media.side_effect=ValueError("test failure")
        t=await self.run_task(p)
        self.assertEqual(t["state"],"failed")
        self.assertEqual(t["progress"],1)
        m.download_media.side_effect=original
        await self.controller.task_command(OWNER,OWNER,"/retry",[t["id"]])
        await self.engine.execute(self.store.claim())
        self.assertEqual(self.store.task(t["id"],OWNER,OWNER)["state"],"done")
        self.client.main[0].download_media.assert_awaited_once()

    async def test_network_send_ambiguous_no_blind_retry(self):
        self.delivery.send.side_effect=NetworkError("timeout")
        t=await self.run_task(await self.payload("forward",[OWNER,None]))
        self.assertEqual(t["state"],"ambiguous")
        with self.assertRaises(ValueError):
            await self.controller.task_command(OWNER,OWNER,"/retry",[t["id"]])
        self.assertIsNone(self.store.claim())
        self.delivery.send.assert_awaited_once()

    async def test_restart_inflight_send_becomes_ambiguous(self):
        p=await self.payload("forward",[OWNER,None])
        t=self.store.create_task(OWNER,OWNER,100,p)
        self.store.claim()
        i=p["items"][0]
        k=digest(["forward",[OWNER,None],i["chat"],i["id"],i.get("media_id", "")])
        self.store.mark(k,"sending")
        self.store.recover()
        await self.engine.execute(self.store.claim())
        self.assertEqual(self.store.task(t["id"],OWNER,OWNER)["state"],"ambiguous")
        self.delivery.send.assert_not_awaited()

    async def test_floodwait_persists_gate_and_bounded_delayed_state(self):
        self.client.main[0].download_media.side_effect=FloodWaitError(None,capture=3)
        t=await self.run_task(await self.payload())
        self.assertEqual(t["state"],"delayed")
        self.assertGreater(float(self.store.get_meta("rate_until")),time.time())
        self.assertIsNone(self.store.claim())
        self.store.set_task(t["id"],due=0,attempt=self.config.retries-1)
        self.store.meta("rate_until",0)
        await self.engine.execute(self.store.claim())
        self.assertEqual(self.store.task(t["id"],OWNER,OWNER)["state"],"failed")

    async def test_size_item_and_disk_guards(self):
        p=await self.payload(count=2)
        for c in (replace(self.config,max_items=1),replace(self.config,max_bytes=7)):
            with self.assertRaises(ValueError):Engine(c,self.store,self.client,self.gate).validate(p)
        with patch("tg_forwarder.control.engine.shutil.disk_usage",return_value=NS(free=0)),self.assertRaises(OSError):
            self.engine.validate(p)

    async def test_actual_size_guard_and_partial_cleanup(self):
        p=await self.payload()
        p["items"][0]["size"]=None
        engine=Engine(replace(self.config,max_bytes=3),self.store,self.client,self.gate)
        t=self.store.create_task(OWNER,OWNER,100,p)
        await engine.execute(self.store.claim())
        self.assertEqual(self.store.task(t["id"],OWNER,OWNER)["state"],"failed")
        self.assertEqual(list(self.config.root.rglob("*.jpg")),[])
        self.assertEqual(list(self.config.root.rglob(".download-*")),[])

    def test_path_traversal_symlink_root_and_components(self):
        for part in ("../outside","..","/tmp/x","a/b","", "x\\y"):
            with self.assertRaises(ValueError):self.engine.archive.path(part)
        link=self.config.root/"link";link.symlink_to(self.root,target_is_directory=True)
        with self.assertRaises(ValueError):self.engine.archive.path("link","escape")
        with self.assertRaises(ValueError):Archive(link,0)

    async def test_existing_untracked_archive_not_overwritten(self):
        p=await self.payload()
        path=self.engine.archive.file(p["snapshot"],p["items"][0]);path.parent.mkdir(parents=True)
        path.write_bytes(b"existing")
        self.assertEqual((await self.run_task(p))["state"],"failed")
        self.assertEqual(path.read_bytes(),b"existing")

    async def test_removed_owner_task_denied(self):
        p=await self.payload()
        t=self.store.create_task(OWNER,OWNER,100,p)
        engine=Engine(replace(self.config,allowed=frozenset()),self.store,self.client,self.gate)
        await engine.execute(self.store.claim())
        self.assertEqual(self.store.task(t["id"],OWNER,OWNER)["state"],"failed")
        self.assertEqual(list(self.config.root.rglob("*.jpg")),[])

    async def test_concurrent_download_cache_serialized(self):
        p=await self.payload("download")
        q={**p,"action":"forward","target":[OWNER,None]}
        self.store.create_task(OWNER,OWNER,100,p)
        self.store.create_task(OWNER,OWNER,100,q)
        await asyncio.gather(self.engine.execute(self.store.claim()),self.engine.execute(self.store.claim()))
        self.client.main[0].download_media.assert_awaited_once()
        self.delivery.send.assert_awaited_once()


class HandlerTests(Base):
    async def callback_update(self,token,uid=OWNER,chat=OWNER,reply=100):
        cb={"id":"cb1","from":{"id":uid,"is_bot":False,"first_name":"actor"},"chat_instance":"abc","data":token,
            "message":{"message_id":reply,"date":1,"chat":{"id":chat,"type":"private"},"from":{"id":123,"is_bot":True,"first_name":"bot"},"text":"preview"}}
        await self.controller.handle(Update.de_json({"update_id":43,"callback_query":cb},None))

    async def test_real_url_handler_edits_same_preview_and_queues_via_bound_button(self):
        await self.controller.handle(self.update("https://t.me/TGFanChabiao/4833"))
        self.assertEqual(self.client.calls,[])
        await self.controller.preview_tick()
        self.bot.send_message.assert_awaited_once()
        edit=self.bot.edit_message_text.call_args.kwargs
        self.assertEqual(edit["message_id"],100)
        token=edit["reply_markup"].inline_keyboard[0][0].callback_data
        await self.callback_update(token)
        self.assertEqual(len(self.store.tasks(OWNER,OWNER)),1)
        await self.callback_update(token)
        self.assertEqual(len(self.store.tasks(OWNER,OWNER)),1)

    async def test_forwarded_input_wired(self):
        await self.controller.handle(self.update("",forward_origin={"type":"channel","date":1,"chat":{"id":CHANNEL,"type":"channel","title":"source"},"message_id":4833}))
        await self.controller.preview_tick()
        self.assertIn("媒體快照",self.bot.edit_message_text.call_args.kwargs["text"])

    async def test_forged_cross_user_chat_reply_callbacks_denied(self):
        snap=await self.snapshot()
        qid=self.store.query(OWNER,OWNER,100,snap)
        token=self.store.capability(OWNER,OWNER,100,{"query":qid,"scope":"single","content":"all","do":"download"})
        for tok,uid,chat,reply in (("forged",OWNER,OWNER,100),(token,88,OWNER,100),(token,OWNER,88,100),(token,OWNER,OWNER,101)):
            await self.callback_update(tok,uid,chat,reply)
        self.assertEqual(self.store.tasks(OWNER,OWNER),[])
        self.assertTrue(self.bot.answer_callback_query.call_args.kwargs["show_alert"])

    async def test_large_confirmation_is_required(self):
        snap=await self.snapshot()
        qid=self.store.query(OWNER,OWNER,100,snap)
        self.controller.config=replace(self.config,confirm_items=1)
        token=self.store.capability(OWNER,OWNER,100,{"query":qid,"scope":"single","content":"all","do":"download"})
        await self.callback_update(token)
        self.assertEqual(self.store.tasks(OWNER,OWNER),[])
        token=self.bot.edit_message_text.call_args.kwargs["reply_markup"].inline_keyboard[0][0].callback_data
        await self.callback_update(token)
        self.assertEqual(len(self.store.tasks(OWNER,OWNER)),1)

    async def test_watch_default_off_explicit_optin_cursor_restart_unsubscribe(self):
        snap=await self.snapshot()
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM watches").fetchone()[0],0)
        qid=self.store.query(OWNER,OWNER,100,snap)
        token=self.store.capability(OWNER,OWNER,100,{"query":qid,"scope":"all","content":"all","do":"watch","action":"download"})
        await self.callback_update(token)
        watch=self.store.db.execute("SELECT * FROM watches").fetchone()
        self.assertEqual(watch["cursor"],24206)
        self.client.replies.append(message(GROUP,24207,kind="video"))
        await self.controller.watch_tick()
        tasks=self.store.tasks(OWNER,OWNER)
        self.assertEqual(len(tasks),1)
        self.assertEqual([i["id"] for i in json.loads(tasks[0]["payload"])["items"]],[24207])
        self.store.recover()
        self.store.db.execute("UPDATE watches SET due=0")
        await self.controller.watch_tick()
        self.assertEqual(len(self.store.tasks(OWNER,OWNER)),1)
        await self.controller.watch_command(OWNER,OWNER,"/unwatch",[watch["id"]])
        self.assertEqual(self.store.db.execute("SELECT state FROM watches").fetchone()[0],"stopped")

    async def test_watch_partial_does_not_advance_cursor(self):
        snap=await self.snapshot()
        self.store.watch(OWNER,OWNER,{"ref":snap["ref"],"action":"download","content":"all"},24200)
        self.client.partial=True
        await self.controller.watch_tick()
        row=self.store.db.execute("SELECT cursor,error FROM watches").fetchone()
        self.assertEqual(row["cursor"],24200)
        self.assertIn("partial",row["error"])

    async def test_watch_owner_removed_denied(self):
        snap=await self.snapshot()
        self.store.watch(88,OWNER,{"ref":snap["ref"],"action":"download","content":"all"},24200)
        await self.controller.watch_tick()
        self.assertEqual(self.store.db.execute("SELECT state FROM watches").fetchone()[0],"denied")

    def test_durable_updates_offset_recovery(self):
        self.store.ingest(self.update("/health").to_dict())
        self.store.ingest(self.update("/health").to_dict())
        self.assertEqual(self.store.get_meta("offset"),"43")
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM updates").fetchone()[0],1)
        self.store.db.execute("UPDATE updates SET state='processing'")
        self.store.recover()
        self.assertEqual(self.store.db.execute("SELECT state FROM updates").fetchone()[0],"pending")

    async def test_durable_inbox_reconstructs_actual_updates(self):
        self.store.ingest(self.update("/health").to_dict())
        worker=asyncio.create_task(inbox_loop(self.controller,self.store))
        for _ in range(20):
            if self.store.db.execute("SELECT state FROM updates").fetchone()[0]=="done":break
            await asyncio.sleep(0.01)
        worker.cancel();await asyncio.gather(worker,return_exceptions=True)
        self.bot.send_message.assert_awaited_once()
        self.assertEqual(self.store.db.execute("SELECT state FROM updates").fetchone()[0],"done")

    async def test_bot_delivery_actual_media_album_caption_order(self):
        p=await self.payload(count=2)
        files=[]
        for n in range(2):
            f=self.root/str(n);f.write_bytes(b"data");files.append(f)
        bot=NS(send_media_group=AsyncMock(return_value=[NS(message_id=1),NS(message_id=2)]))
        await BotDelivery(bot).send([OWNER,None],p["items"],files)
        args=bot.send_media_group.call_args.kwargs
        self.assertEqual(args["chat_id"],OWNER)
        self.assertEqual([m.caption for m in args["media"]],["caption 4833","caption 4834"])
        with self.assertRaises(ValueError):await BotDelivery(bot,max_bytes=3).send([OWNER,None],p["items"],files)

    def test_main_early_control_dispatch_before_legacy_env(self):
        source=Path("main.py").read_text()
        self.assertLess(source.index("from tg_forwarder.control.runtime import cli"),source.index("API_ID_RAW"))
        self.assertIn('if __name__ == "__main__" and os.getenv("APP_MODE", "listen").strip().lower() == "control"',source)


class RecoveryTests(Base):
    async def test_full_70_media_download_scope_has_no_forwarded_root_duplicate(self):
        snap=await self.snapshot()
        p={"snapshot":snap,"items":scopes(snap)["all"],"action":"download"}
        t=await self.run_task(p)
        self.assertEqual(t["state"],"done",t["error"])
        self.assertEqual(t["progress"],70)
        self.assertEqual(len(list(self.config.root.rglob("*.jpg"))),59)
        self.assertEqual(len(list(self.config.root.rglob("*.mp4"))),11)
        for root in self.client.roots:root.download_media.assert_not_awaited()
        self.assertFalse(any(p.name.startswith("-1003837422582_24124") for p in self.config.root.rglob("*")))

    async def test_forward_concurrent_overlapping_tasks_claim_once(self):
        p=await self.payload("forward",[OWNER,None],1)
        q={**p,"action":"both"}
        self.store.create_task(OWNER,OWNER,100,p)
        self.store.create_task(OWNER,OWNER,100,q)
        async def delayed_send(target,group,files):
            await asyncio.sleep(0.02)
            return [NS(message_id=1)]
        self.delivery.send.side_effect=delayed_send
        await asyncio.gather(self.engine.execute(self.store.claim()),self.engine.execute(self.store.claim()))
        self.delivery.send.assert_awaited_once()
        self.assertTrue(all(t["state"]=="done" for t in self.store.tasks(OWNER,OWNER)))

    async def test_preview_survives_restart_without_blocking_tasks_command(self):
        await self.controller.handle(self.update("https://t.me/TGFanChabiao/4833"))
        self.assertEqual(self.client.calls,[])
        await self.controller.handle(self.update("/tasks"))
        self.assertIn("暫無任務",self.bot.send_message.call_args.kwargs["text"])
        self.store.db.execute("UPDATE previews SET state='running'")
        self.store.recover()
        await self.controller.preview_tick()
        self.assertEqual(self.store.db.execute("SELECT state FROM previews").fetchone()[0],"done")
        self.assertEqual(self.bot.edit_message_text.call_args.kwargs["message_id"],100)

    async def test_callback_and_queries_survive_database_reopen(self):
        snap=await self.snapshot()
        qid=self.store.query(OWNER,OWNER,100,snap)
        cap=self.store.capability(OWNER,OWNER,100,{"query":qid})
        other=Store(self.config.db)
        try:
            self.assertEqual(other.use(cap,OWNER,OWNER,100),{"query":qid})
            self.assertEqual(other.get_query(qid,OWNER,OWNER,100)["post"],4833)
        finally:other.close()

    async def test_watch_resubscribe_and_unknown_size_waits_manual_confirmation(self):
        snap=await self.snapshot()
        p={"ref":snap["ref"],"action":"download","content":"all"}
        wid=self.store.watch(OWNER,OWNER,p,snap["cursor"])
        await self.controller.watch_command(OWNER,OWNER,"/unwatch",[wid])
        self.store.watch(OWNER,OWNER,p,snap["cursor"])
        self.assertEqual(self.store.db.execute("SELECT state FROM watches").fetchone()[0],"active")
        m=message(GROUP,24207);m.file.size=None;self.client.replies.append(m)
        await self.controller.watch_tick()
        row=self.store.db.execute("SELECT cursor,error FROM watches").fetchone()
        self.assertEqual(row["cursor"],24206)
        self.assertIn("確認",row["error"])
        self.assertEqual(self.store.tasks(OWNER,OWNER),[])

    async def test_bot_retryafter_is_delayed_not_ambiguous(self):
        self.delivery.send.side_effect=RetryAfter(2)
        t=await self.run_task(await self.payload("forward",[OWNER,None]))
        self.assertEqual(t["state"],"delayed")
        self.assertIn("2",t["error"])

    async def test_caption_limit_fails_without_truncation(self):
        p=await self.payload()
        p["items"][0]["caption"]="x"*1025
        path=self.root/"media.jpg";path.write_bytes(b"data")
        bot=NS(send_photo=AsyncMock())
        with self.assertRaisesRegex(ValueError,"1024"):
            await BotDelivery(bot).send([OWNER,None],p["items"],[path])
        bot.send_photo.assert_not_awaited()
        self.assertEqual(len(p["items"][0]["caption"]),1025)

    async def test_source_changed_media_identity_rejected(self):
        self.client.main[0].photo=NS(id=100)
        p=await self.payload()
        self.client.main[0].photo=NS(id=101)
        t=await self.run_task(p)
        self.assertEqual(t["state"],"failed")
        self.assertIn("已更改",t["error"])
        self.client.main[0].download_media.assert_not_awaited()

    async def test_restart_uses_persisted_access_hash(self):
        self.client.channel.access_hash=789
        p=await self.payload()
        peer=self.engine.peer(p["items"][0])
        self.assertEqual(peer.channel_id,4433596337)
        self.assertEqual(peer.access_hash,789)

    async def test_state_local_readonly_inspection_uses_existing_resolver(self):
        from tg_forwarder.control.inspection import serve, inspect_running, socket_path, health_running
        original_connect=self.net.temp_original
        self.net.stop()
        def only_unix(sock,address):
            if sock.family != socket.AF_UNIX:raise AssertionError("network forbidden")
            return original_connect(sock,address)
        try:
            with patch.object(socket.socket,"connect",only_unix):
                server=asyncio.create_task(serve(self.config,self.resolver))
                try:
                    for _ in range(30):
                        if socket_path(self.config).exists():break
                        await asyncio.sleep(0.01)
                    self.assertEqual(socket_path(self.config).stat().st_mode & 0o777,0o600)
                    calls=len(self.client.calls)
                    reader,writer=await asyncio.open_unix_connection(str(socket_path(self.config)))
                    writer.write(b'{"health":true}\n');await writer.drain()
                    self.assertEqual(json.loads(await reader.readline()),{"status":"ok"})
                    writer.close();await writer.wait_closed()
                    self.assertEqual(await health_running(self.config),{"status":"ok"})
                    self.assertEqual(len(self.client.calls),calls)
                    result=await inspect_running(self.config,"https://t.me/TGFanChabiao/4833?comment=24204")
                    self.assertEqual(result["counts"]["all"],70)
                    self.assertEqual(result["counts"]["album"],10)
                    self.assertEqual(result["photos"],59)
                    self.delivery.send.assert_not_awaited()
                    self.assertEqual(list(self.config.root.rglob("*.jpg")),[])
                finally:
                    server.cancel();await asyncio.gather(server,return_exceptions=True)
                self.assertFalse(socket_path(self.config).exists())
        finally:self.net.start()

    async def test_progress_throttled_terminal_always_notified(self):
        calls=[]
        async def notify(task):calls.append(task["state"])
        self.engine.notify=notify
        t=await self.run_task(await self.payload(count=10))
        self.assertEqual(t["state"],"done")
        self.assertEqual(calls,["running","done"])

    async def test_control_session_not_inferred_from_listener(self):
        from tg_forwarder.control.runtime import client_from_env
        with patch.dict(os.environ,{"API_ID":"123","API_HASH":"unused","SESSION_NAME":"listener"},clear=True),self.assertRaisesRegex(RuntimeError,"exactly one"):
            client_from_env()
        session=self.root/"listener.session";session.write_bytes(b"not a session")
        with patch.dict(os.environ,{"API_ID":"123","API_HASH":"unused","SESSION_NAME":str(self.root/"listener"),"MEDIA_USER_SESSION_FILE":str(session)},clear=True),self.assertRaises(RuntimeError):
            client_from_env()

    async def test_cancel_during_download_stops_before_publication(self):
        p=await self.payload()
        task=self.store.create_task(OWNER,OWNER,100,p)
        async def interrupted(file,progress_callback):
            file.write(b"data")
            self.store.set_task(task["id"],state="cancelled")
            await progress_callback(4,4)
        self.client.main[0].download_media.side_effect=interrupted
        await self.engine.execute(self.store.claim())
        self.assertEqual(self.store.task(task["id"],OWNER,OWNER)["state"],"cancelled")
        self.assertEqual(list(self.config.root.rglob("*.jpg")),[])
        self.assertEqual(list(self.config.root.rglob(".download-*")),[])

    async def test_delete_source_after_archive_does_not_delete_archive(self):
        p=await self.payload()
        t=await self.run_task(p)
        files=list(self.config.root.rglob("*.jpg"))
        self.assertEqual(len(files),1)
        self.client.main=[]
        self.assertEqual((await self.run_task(p))["state"],"done")
        self.assertTrue(files[0].exists())


class AdditionalSafetyTests(Base):
    async def test_cached_media_rechecks_current_message_protection(self):
        p=await self.payload()
        await self.run_task(p)
        self.client.main[0].noforwards=True
        p={**p,"action":"forward","target":[OWNER,None]}
        t=await self.run_task(p)
        self.assertEqual(t["state"],"failed")
        self.assertIn("禁止轉發",t["error"])
        self.delivery.send.assert_not_awaited()
        self.assertEqual(len(list(self.config.root.rglob("*.jpg"))),1)

    async def test_new_media_revision_does_not_reuse_old_task_or_bytes(self):
        self.client.main[0].photo=NS(id=100)
        p=await self.payload()
        first=await self.run_task(p)
        self.client.main[0].photo=NS(id=101)
        second=await self.run_task(await self.payload())
        self.assertNotEqual(first["id"],second["id"])
        self.assertEqual(second["state"],"done")
        self.assertEqual(len(list(self.config.root.rglob("*.jpg"))),2)
        self.assertEqual(self.client.main[0].download_media.await_count,2)

    async def test_manifest_excludes_peer_access_hash(self):
        self.client.channel.access_hash=789
        t=await self.run_task(await self.payload())
        self.assertEqual(t["state"],"done")
        data=json.loads(next(self.config.root.rglob("manifest_*.json")).read_text())
        self.assertNotIn("access_hash",json.dumps(data))

    async def test_reply_rate_limit_propagates_to_account_gate(self):
        original=self.client.__class__.__call__
        async def flood(client,req):
            if isinstance(req,functions.messages.GetRepliesRequest):
                raise FloodWaitError(None,capture=2)
            return await original(client,req)
        with patch.object(FakeClient,"__call__",flood),self.assertRaises(FloodWaitError):
            await self.snapshot()
        self.assertGreater(float(self.store.get_meta("rate_until")),time.time())


class ForwardedAlbumInputTests(Base):
    async def test_forwarded_album_members_share_one_durable_preview(self):
        for mid in range(4833,4843):
            update=self.update("",media_group_id="source-album",forward_origin={"type":"channel","date":1,"chat":{"id":CHANNEL,"type":"channel","title":"source"},"message_id":mid})
            await self.controller.handle(update)
        self.bot.send_message.assert_awaited_once()
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM previews").fetchone()[0],1)
        await self.controller.preview_tick()
        self.assertIn("所選相簿10",self.bot.edit_message_text.call_args.kwargs["text"])


class TaskUiTests(Base):
    callback_update = HandlerTests.callback_update

    def buttons(self):
        return [b for row in self.bot.edit_message_text.call_args.kwargs["reply_markup"].inline_keyboard for b in row]

    def token(self, label):
        return next(b.callback_data for b in self.buttons() if label in b.text)

    async def task_with_controls(self, state="queued", count=2, action="download"):
        p=await self.payload(action,[OWNER,None] if action!="download" else None,count)
        task=self.store.create_task(OWNER,OWNER,100,p)
        self.store.set_task(task["id"],state=state)
        await self.controller.render_task(OWNER,OWNER,100,task)
        return self.store.task(task["id"],OWNER,OWNER)

    async def test_actual_details_cancel_resume_callbacks(self):
        task=await self.task_with_controls()
        details=self.token("詳情")
        await self.callback_update(details)
        self.assertIn("已完成 0/2",self.bot.edit_message_text.call_args.kwargs["text"])
        await self.callback_update(self.token("取消"))
        self.assertEqual(self.store.task(task["id"],OWNER,OWNER)["state"],"cancelled")
        self.assertIn("失敗／未完成餘下 2",self.bot.edit_message_text.call_args.kwargs["text"])
        await self.callback_update(self.token("繼續"))
        self.assertEqual(self.store.task(task["id"],OWNER,OWNER)["state"],"queued")
        self.assertTrue(any(b.text=="取消" for b in self.buttons()))

    async def test_failed_retry_callback_reuses_completed_downloads(self):
        p=await self.payload(count=2)
        original=self.client.main[1].download_media.side_effect
        self.client.main[1].download_media.side_effect=ValueError("fixture")
        task=await self.run_task(p)
        await self.controller.render_task(OWNER,OWNER,100,task)
        self.assertIn("已完成 1/2",self.bot.edit_message_text.call_args.kwargs["text"])
        await self.callback_update(self.token("重試失敗"))
        self.assertEqual(self.store.task(task["id"],OWNER,OWNER)["state"],"queued")
        self.client.main[1].download_media.side_effect=original
        await self.engine.execute(self.store.claim())
        self.client.main[0].download_media.assert_awaited_once()
        await self.controller.render_task(OWNER,OWNER,100,task)
        self.assertIn("已完成 2/2",self.bot.edit_message_text.call_args.kwargs["text"])
        self.assertIn("未完成餘下 0",self.bot.edit_message_text.call_args.kwargs["text"])
        self.assertFalse(any(b.text in ("取消","繼續","重試失敗項目") for b in self.buttons()))

    async def test_notify_uses_render_task_and_keeps_live_controls(self):
        task=await self.task_with_controls(count=1)
        notices=[]
        async def notify(row):
            await self.controller.render_task(row["owner"],row["chat"],row["reply"],row)
            notices.append([b.text for b in self.buttons()])
        self.engine.notify=notify
        await self.engine.execute(self.store.claim())
        self.assertIn("取消",notices[0])
        self.assertIn("詳情／更新",notices[-1])
        self.assertNotIn("取消",notices[-1])
        runtime=Path("tg_forwarder/control/runtime.py").read_text()
        self.assertIn('await controller.render_task(task["owner"], task["chat"]',runtime)

    async def test_tasks_command_lists_bounded_owner_chat_bound_buttons(self):
        p=await self.payload()
        for n in range(22):
            p={**p,"items":[{**p["items"][0],"id":5000+n}]}
            self.store.create_task(OWNER,OWNER,100,p)
        self.store.create_task(88,OWNER,100,p)
        await self.controller.handle(self.update("/tasks"))
        markup=self.bot.edit_message_text.call_args.kwargs["reply_markup"]
        self.assertEqual(len(markup.inline_keyboard),20)
        for row in markup.inline_keyboard:
            capability=self.store.use(row[0].callback_data,OWNER,OWNER,100)
            self.assertEqual(capability["kind"],"task")
            self.assertEqual(self.store.task(capability["task"],OWNER,OWNER)["owner"],OWNER)
        await self.callback_update(markup.inline_keyboard[0][0].callback_data)
        self.assertIn("已完成 0/1",self.bot.edit_message_text.call_args.kwargs["text"])
        await self.callback_update(self.token("我的任務"))
        self.assertIn("最多20項",self.bot.edit_message_text.call_args.kwargs["text"])

    async def test_task_controls_denied_cross_user_chat_reply_expired_and_other_owner(self):
        task=await self.task_with_controls()
        cancel=self.token("取消")
        for uid,chat,reply in ((88,OWNER,100),(OWNER,88,100),(OWNER,OWNER,101)):
            await self.callback_update(cancel,uid,chat,reply)
        self.store.db.execute("UPDATE callbacks SET expires=0 WHERE id=?",(cancel,))
        await self.callback_update(cancel)
        self.assertEqual(self.store.task(task["id"],OWNER,OWNER)["state"],"queued")
        # Even an otherwise valid owner-bound capability cannot reference another owner's task.
        p=await self.payload()
        other=self.store.create_task(88,OWNER,100,p)
        token=self.store.capability(OWNER,OWNER,100,{"kind":"task","task":other["id"],"do":"cancel"})
        await self.callback_update(token)
        self.assertEqual(self.store.task(other["id"],88,OWNER)["state"],"queued")

    async def test_ambiguous_has_details_but_no_retry_or_resume(self):
        task=await self.task_with_controls("ambiguous",action="forward")
        self.assertIn("禁止盲目重送",self.bot.edit_message_text.call_args.kwargs["text"])
        self.assertEqual([b.text for b in self.buttons()],["詳情／更新","我的任務"])
        token=self.store.capability(OWNER,OWNER,100,{"kind":"task","task":task["id"],"do":"retry"})
        await self.callback_update(token)
        self.assertEqual(self.store.task(task["id"],OWNER,OWNER)["state"],"ambiguous")
        self.delivery.send.assert_not_awaited()

    async def test_forward_progress_is_not_download_completion(self):
        task=await self.task_with_controls("failed",action="forward")
        self.store.set_task(task["id"],progress=2)
        await self.controller.render_task(OWNER,OWNER,100,task)
        text=self.bot.edit_message_text.call_args.kwargs["text"]
        self.assertIn("已完成 0/2；已下載 2/2",text)
        self.assertIn("未完成餘下 2",text)

    async def test_summary_comment_type_album_and_discussion_composition(self):
        await self.controller.handle(self.update("https://t.me/TGFanChabiao/4833?comment=24204"))
        await self.controller.preview_tick()
        text=self.bot.edit_message_text.call_args.kwargs["text"]
        self.assertIn("所選留言單項：影片",text)
        self.assertIn("所選留言相簿10（6相＋4片）",text)
        self.assertIn("整串留言60（49相＋11片）",text)
        self.assertIn("原帖＋留言70（59相＋11片）",text)

    async def test_actions_watch_confirmation_back_buttons_preserve_snapshot(self):
        snap=await self.snapshot()
        qid=self.store.query(OWNER,OWNER,100,snap)
        action=self.store.capability(OWNER,OWNER,100,{"query":qid,"scope":"album","content":"all","do":"actions"})
        await self.callback_update(action)
        await self.callback_update(self.token("返回範圍"))
        self.assertIn("媒體快照",self.bot.edit_message_text.call_args.kwargs["text"])
        await self.callback_update(action)
        await self.callback_update(self.token("新留言監察"))
        await self.callback_update(self.token("返回動作"))
        self.assertIn("選擇動作",self.bot.edit_message_text.call_args.kwargs["text"])
        self.controller.config=replace(self.config,confirm_items=1)
        await self.callback_update(self.token("下載"))
        self.assertIn("確認？",self.bot.edit_message_text.call_args.kwargs["text"])
        await self.callback_update(self.token("返回動作"))
        self.assertIn("選擇動作",self.bot.edit_message_text.call_args.kwargs["text"])
        self.assertEqual(self.store.get_query(qid,OWNER,OWNER,100),snap)
        self.assertEqual(self.store.tasks(OWNER,OWNER),[])


class IndependentReviewRegressionTests(Base):
    async def subscribe(self, comment=None):
        snap=await self.snapshot(comment)
        p=self.controller.watch_subscription(snap,"download",None,"all")
        wid=self.store.watch(OWNER,OWNER,p,snap["cursor"])
        return wid

    async def test_preview_missing_message_does_not_kill_worker(self):
        from telegram.error import BadRequest
        await self.controller.handle(self.update("https://t.me/TGFanChabiao/4833"))
        self.bot.edit_message_text.side_effect=BadRequest("Message to edit not found")
        worker=asyncio.create_task(self.controller.preview_loop())
        try:
            for _ in range(100):
                if self.store.db.execute("SELECT state FROM previews").fetchone()[0]=="failed":break
                await asyncio.sleep(.01)
            self.assertFalse(worker.done())
            self.assertEqual(self.store.db.execute("SELECT state FROM previews").fetchone()[0],"failed")
            self.bot.edit_message_text.side_effect=None
            await self.controller.handle(self.update("/health"))
            self.assertIn("已獲授權",self.bot.send_message.call_args.kwargs["text"])
        finally:
            worker.cancel();await asyncio.gather(worker,return_exceptions=True)

    async def test_source_error_notification_failure_is_isolated(self):
        from telegram.error import BadRequest
        await self.controller.handle(self.update("https://t.me/TGFanChabiao/4833"))
        self.resolver.resolve=AsyncMock(side_effect=RuntimeError("source unavailable"))
        self.bot.edit_message_text.side_effect=BadRequest("missing")
        await self.controller.preview_tick()
        self.assertEqual(self.store.db.execute("SELECT state FROM previews").fetchone()[0],"failed")

    async def test_preview_retryafter_retains_snapshot_and_bounds_retries(self):
        await self.controller.handle(self.update("https://t.me/TGFanChabiao/4833"))
        self.bot.edit_message_text.side_effect=RetryAfter(2)
        await self.controller.preview_tick()
        calls=len(self.client.calls)
        self.assertEqual(self.store.db.execute("SELECT state FROM previews").fetchone()[0],"pending")
        self.assertGreater(float(self.store.get_meta("rate_until")),time.time())
        for _ in range(self.config.retries-1):
            self.store.db.execute("UPDATE previews SET due=0")
            self.store.meta("rate_until",0)
            await self.controller.preview_tick()
        self.assertEqual(len(self.client.calls),calls)
        self.assertEqual(self.store.db.execute("SELECT state FROM previews").fetchone()[0],"failed")

    async def test_unwatch_during_awaited_resolution_prevents_task_and_cursor_commit(self):
        wid=await self.subscribe()
        self.client.replies.append(message(GROUP,24207))
        original=self.resolver.new_comments
        entered,release=asyncio.Event(),asyncio.Event()
        async def paused(*args):
            entered.set();await release.wait();return await original(*args)
        self.resolver.new_comments=paused
        tick=asyncio.create_task(self.controller.watch_tick())
        await entered.wait()
        await self.controller.handle(self.update("/unwatch "+wid))
        release.set();await tick
        row=self.store.db.execute("SELECT * FROM watches WHERE id=?",(wid,)).fetchone()
        self.assertEqual((row["state"],row["cursor"]),("stopped",24206))
        self.assertEqual(self.store.tasks(OWNER,OWNER),[])

    async def test_watch_generation_change_invalidates_inflight_observation(self):
        wid=await self.subscribe()
        before=self.store.db.execute("SELECT * FROM watches").fetchone()
        self.store.stop_watch(wid,OWNER,OWNER)
        self.store.watch(OWNER,OWNER,json.loads(before["payload"]),before["cursor"])
        p=await self.payload()
        self.assertFalse(self.store.commit_watch(before,cursor=24207,task=p))
        self.assertEqual(self.store.tasks(OWNER,OWNER),[])

    async def test_comment_watch_uses_parent_thread_after_selected_comment_deleted(self):
        wid=await self.subscribe(24204)
        p=json.loads(self.store.db.execute("SELECT payload FROM watches").fetchone()[0])
        self.assertIsNone(p["ref"]["comment"])
        self.assertEqual(p["ref"]["post"],4833)
        self.client.replies=[m for m in self.client.replies if m.id!=24204]
        self.client.replies.append(message(GROUP,24207,kind="video"))
        self.resolver.resolve=AsyncMock(side_effect=AssertionError("must not resolve selected comment/history"))
        await self.controller.watch_tick()
        task=self.store.tasks(OWNER,OWNER)[0]
        self.assertEqual([i["id"] for i in json.loads(task["payload"])["items"]],[24207])
        self.assertEqual(self.store.db.execute("SELECT cursor FROM watches").fetchone()[0],24207)
        self.resolver.resolve.assert_not_awaited()

    async def test_incremental_watch_does_not_rescan_over_cap_history_and_exact_cap_complete(self):
        await self.subscribe()
        self.resolver.limit=3
        self.client.replies += [message(GROUP,24207+n) for n in range(3)]
        self.client.calls=[]
        await self.controller.watch_tick()
        row=self.store.db.execute("SELECT cursor,error FROM watches").fetchone()
        self.assertEqual(row["cursor"],24209,row["error"])
        requests=self.client.calls
        self.assertTrue(all(isinstance(r,functions.messages.GetRepliesRequest) and r.min_id==24206 for r in requests))
        self.assertEqual(len(requests),2)
        self.assertEqual(len(json.loads(self.store.tasks(OWNER,OWNER)[0]["payload"])["items"]),3)

    async def test_full_scan_exact_limit_complete_not_partial(self):
        self.resolver.limit=len(self.client.roots+self.client.replies)
        snap=await self.snapshot()
        self.assertFalse(snap["partial"],snap["warning"])
        self.assertEqual(len(scopes(snap)["all"]),70)

    async def test_incremental_cap_exceeded_does_not_commit_cursor(self):
        await self.subscribe()
        self.resolver.limit=2
        self.client.replies += [message(GROUP,24207+n) for n in range(3)]
        await self.controller.watch_tick()
        row=self.store.db.execute("SELECT cursor,error FROM watches").fetchone()
        self.assertEqual(row["cursor"],24206)
        self.assertIn("partial",row["error"])
        self.assertEqual(self.store.tasks(OWNER,OWNER),[])

    async def test_arriving_album_settles_before_atomic_cursor_commit_and_survives_restart(self):
        with patch("tg_forwarder.control.controller.time.time",return_value=1000):
            await self.subscribe()
            self.client.replies.append(message(GROUP,24207,album=999))
            await self.controller.watch_tick()
            self.assertEqual(self.store.tasks(OWNER,OWNER),[])
        with patch("tg_forwarder.control.controller.time.time",return_value=1031):
            self.client.replies.append(message(GROUP,24208,album=999))
            await self.controller.watch_tick()
            self.assertEqual(self.store.tasks(OWNER,OWNER),[])
            self.assertEqual(self.store.db.execute("SELECT cursor FROM watches").fetchone()[0],24206)
            self.store.recover()
        with patch("tg_forwarder.control.controller.time.time",return_value=1062):
            await self.controller.watch_tick()
            task=self.store.tasks(OWNER,OWNER)[0]
            self.assertEqual([i["id"] for i in json.loads(task["payload"])["items"]],[24207,24208])
            self.assertEqual(self.store.db.execute("SELECT cursor FROM watches").fetchone()[0],24208)
            self.assertEqual(len(self.store.tasks(OWNER,OWNER)),1)

    async def test_none_empty_and_truncated_downloads_never_cache_success(self):
        for mode in ("none","empty","truncated"):
            p=await self.payload()
            p["items"][0]["id"]=6000+len(mode)
            m=message(CHANNEL,p["items"][0]["id"],album=1)
            self.client.main.append(m)
            async def bad_download(file,progress_callback,mode=mode):
                if mode!="empty":file.write(b"data" if mode=="none" else b"bad")
                return None if mode=="none" else file
            m.download_media.side_effect=bad_download
            task=await self.run_task(p)
            self.assertEqual(task["state"],"failed",mode)
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM cache").fetchone()[0],0)
        self.assertEqual(list(self.config.root.rglob("*.jpg")),[])
        self.assertEqual(list(self.config.root.rglob(".download-*")),[])

    def test_forbidden_ingress_advances_offset_without_retaining_payload(self):
        from tg_forwarder.control.runtime import ingest_authorized
        denied=self.update("https://t.me/TGFanChabiao/4833",uid=88)
        self.assertFalse(ingest_authorized(denied,self.store,self.config.allowed))
        self.assertEqual(self.store.get_meta("offset"),"43")
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM updates").fetchone()[0],0)
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM previews").fetchone()[0],0)
        self.assertEqual(self.client.calls,[])

    def test_bot_and_sender_chat_ingress_do_not_persist_raw(self):
        from tg_forwarder.control.runtime import ingest_authorized
        updates=[self.update("/tasks",sender_chat={"id":-1002,"type":"channel","title":"spoof"}),
                 self.update("/tasks",**{"from":{"id":OWNER,"is_bot":True,"first_name":"bot"}})]
        for update in updates:self.assertFalse(ingest_authorized(update,self.store,self.config.allowed))
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM updates").fetchone()[0],0)
        self.assertEqual(self.store.get_meta("offset"),"43")

    async def test_empty_allowlist_denies_persisted_preview_watch_and_tasks(self):
        await self.controller.handle(self.update("https://t.me/TGFanChabiao/4833"))
        await self.subscribe()
        p=await self.payload()
        task=self.store.create_task(OWNER,OWNER,100,p)
        calls=len(self.client.calls)
        denied=replace(self.config,allowed=frozenset())
        engine=Engine(denied,self.store,self.client,self.gate)
        controller=Controller(denied,self.store,self.resolver,engine,self.bot)
        await controller.preview_tick();await controller.watch_tick();await engine.execute(self.store.claim())
        self.assertEqual(len(self.client.calls),calls)
        self.assertEqual(self.store.db.execute("SELECT state FROM previews").fetchone()[0],"denied")
        self.assertEqual(self.store.db.execute("SELECT state FROM watches").fetchone()[0],"denied")
        self.assertEqual(self.store.task(task["id"],OWNER,OWNER)["state"],"failed")


class FilesystemGuardTests(Base):
    def test_missing_expected_mount_fails_before_archive_mkdir(self):
        from tg_forwarder.control.filesystems import FilesystemError
        mount=self.root/"missing-mount"
        archive=mount/"archive"
        with patch("tg_forwarder.control.filesystems.mounts",return_value=[(Path("/"),"ext4")]),self.assertRaises(FilesystemError):
            Archive(archive,0,mount,("nfs","nfs4"))
        self.assertFalse(mount.exists())

    def test_docker_bind_mount_on_local_filesystem_is_not_nfs_proof(self):
        from tg_forwarder.control.filesystems import guard_archive,FilesystemError
        rows=[(Path("/"),"overlay"),(Path("/media"),"ext4")]
        with patch("tg_forwarder.control.filesystems.mounts",return_value=rows),self.assertRaises(FilesystemError):
            guard_archive(Path("/media/TG_Download"),Path("/media"),("nfs","nfs4"))
        rows[-1]=(Path("/media"),"nfs4")
        with patch("tg_forwarder.control.filesystems.mounts",return_value=rows):
            guard_archive(Path("/media/TG_Download"),Path("/media"),("nfs","nfs4"))

    def test_state_rejects_network_filesystems_before_sqlite_create(self):
        from tg_forwarder.control.filesystems import FilesystemError
        state=self.root/"new-state"/"control.db"
        for kind in ("nfs","nfs4","cifs","smb3","fuse.sshfs","9p"):
            with patch("tg_forwarder.control.filesystems.mounts",return_value=[(Path("/"),kind)]),self.assertRaises(FilesystemError):
                Store(state)
        self.assertFalse(state.parent.exists())

    def test_runtime_guard_precedes_state_mkdir_and_auth_client(self):
        from tg_forwarder.control.filesystems import FilesystemError
        from tg_forwarder.control.runtime import run
        c=replace(self.config,db=self.root/"absent-state"/"control.db",root=self.root/"absent-mount"/"archive",expected_archive_mount=self.root/"absent-mount")
        async def check():
            with patch("tg_forwarder.control.runtime.environment",return_value=c),patch("tg_forwarder.control.runtime.client_from_env") as factory,patch("tg_forwarder.control.filesystems.mounts",return_value=[(Path("/"),"ext4")]),self.assertRaises(FilesystemError):
                await run()
            factory.assert_not_called()
        asyncio.run(check())
        self.assertFalse(c.db.parent.exists())
        self.assertFalse(c.root.parent.exists())

    def test_mountinfo_parses_actual_fstype_and_escaped_mountpoint(self):
        from tg_forwarder.control.filesystems import mounts
        data="1 0 8:1 / / rw - ext4 /dev/sda rw\n2 1 0:50 / /media\\040archive rw - nfs4 nas:/volume1/video rw\n"
        with patch("tg_forwarder.control.filesystems.Path.read_text",return_value=data):
            self.assertEqual(mounts(),[(Path("/"),"ext4"),(Path("/media archive"),"nfs4")])

    def test_config_exact_mount_contract(self):
        with patch.dict(os.environ,{"CONTROL_EXPECTED_ARCHIVE_MOUNT":"/media","CONTROL_EXPECTED_ARCHIVE_FSTYPES":"nfs,nfs4"},clear=True):
            c=Config.from_env()
            self.assertEqual(c.expected_archive_mount,Path("/media"))
            self.assertEqual(c.expected_archive_fstypes,("nfs","nfs4"))


class WatchTailAndIntegrityTests(Base):
    async def test_closed_albums_commit_while_newest_album_is_still_settling(self):
        snap=await self.snapshot()
        p=self.controller.watch_subscription(snap,"download",None,"all")
        with patch("tg_forwarder.control.controller.time.time",return_value=1000):
            self.store.watch(OWNER,OWNER,p,snap["cursor"])
            self.client.replies += [message(GROUP,24207+n,album=999) for n in range(10)]
            self.client.replies.append(message(GROUP,24217,album=1000))
            await self.controller.watch_tick()
        task=self.store.tasks(OWNER,OWNER)[0]
        self.assertEqual([i["id"] for i in json.loads(task["payload"])["items"]],list(range(24207,24217)))
        row=self.store.db.execute("SELECT cursor,settle_signature,error FROM watches").fetchone()
        self.assertEqual(row["cursor"],24216)
        self.assertTrue(row["settle_signature"])
        self.assertIn("等待",row["error"])
        with patch("tg_forwarder.control.controller.time.time",return_value=1031):
            await self.controller.watch_tick()
        self.assertEqual(len(self.store.tasks(OWNER,OWNER)),2)
        self.assertEqual(self.store.db.execute("SELECT cursor FROM watches").fetchone()[0],24217)

    async def test_progress_total_rejects_truncated_unknown_size(self):
        self.client.main[0].file.size=None
        p=await self.payload()
        async def truncated(file,progress_callback):
            file.write(b"bad");await progress_callback(3,4);return file
        self.client.main[0].download_media.side_effect=truncated
        task=await self.run_task(p)
        self.assertEqual(task["state"],"failed")
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM cache").fetchone()[0],0)


class ControlSharedBridgeTests(Base):
    def setUp(self):
        super().setUp()
        from tg_forwarder.shared_media import SharedMedia
        from test_shared_media import Redis
        self.redis = Redis()
        self.bridge = SharedMedia(self.redis, self.root/'shared', account_rate_key='same-account',
                                  min_free_bytes=0, rate_interval=.001)
        for msg in self.client.main:
            msg.photo = NS(id=800000+msg.id)
            msg.text = msg.message
            msg.document = None
            msg.file.mime_type = "image/jpeg"
        self.gate = RateGate(self.store, self.bridge)
        self.engine = Engine(self.config,self.store,self.client,self.gate,delivery=self.delivery,
                             bridge=self.bridge,bot_id=6317800517)
        self.resolver = Resolver(self.client,gate=self.gate)

    async def test_legacy_snapshot_bytes_import_without_control_download(self):
        from tg_forwarder.processing import snapshot
        msg=self.client.main[0]
        async def legacy_download(path):
            Path(path).write_bytes(b'data'); return path
        msg.download_media.side_effect=legacy_download
        async def api(fn,**kw): return await fn()
        with patch.object(snapshot,'configured_bridge',AsyncMock(return_value=self.bridge)):
            snap=await snapshot.snapshot_media_message(msg,{'chat_id':CHANNEL,'msg_id':msg.id},
                'new',self.root/'spool',asyncio.Semaphore(1),api,lambda _:None)
        msg.download_media.reset_mock()
        task=await self.run_task(await self.payload())
        self.assertEqual(task['state'],'done')
        msg.download_media.assert_not_awaited()
        path=Path(self.store.db.execute('SELECT path FROM cache').fetchone()[0])
        self.assertEqual(path.read_bytes(),b'data')
        Path(snap['path']).unlink()
        self.assertEqual(path.read_bytes(),b'data')
        self.assertEqual(list(self.config.root.rglob('.shared-*')),[])

    async def test_control_publish_available_to_real_legacy_snapshot(self):
        from tg_forwarder.processing import snapshot
        self.assertEqual((await self.run_task(await self.payload()))['state'],'done')
        msg=self.client.main[0];msg.download_media.reset_mock()
        async def api(fn,**kw): return await fn()
        with patch.object(snapshot,'configured_bridge',AsyncMock(return_value=self.bridge)):
            snap=await snapshot.snapshot_media_message(msg,{'chat_id':CHANNEL,'msg_id':msg.id},
                'new',self.root/'spool',asyncio.Semaphore(1),api,lambda _:None)
        msg.download_media.assert_not_awaited()
        Path(snap['path']).unlink()
        self.assertEqual((await self.bridge.cached(snap['shared_identity'])).read_bytes(),b'data')
        self.assertTrue(next(self.config.root.rglob('*.jpg')).is_file())

    async def test_receipt_replay_normalizes_ids_and_rebuilds_local_done_without_resend(self):
        p=await self.payload('forward',[OWNER,None],count=2)
        row=await self.run_task(p);self.assertEqual(row['state'],'done')
        self.store.db.execute('DELETE FROM operations')
        task=self.store.task(row['id'],OWNER,OWNER)
        files=[self.engine.archive.file(p['snapshot'],i) for i in p['items']]
        ids=await self.engine.send(task,p,p['items'],files)
        self.assertEqual(ids,[900,901]);self.delivery.send.assert_awaited_once()
        for i in p['items']:
            self.assertEqual(self.store.operation(self.engine.operation_key(p,i))[0],'done')

    async def test_bot_target_topic_caption_and_group_have_distinct_receipts(self):
        p=await self.payload('forward',[OWNER,None],count=2)
        row=await self.run_task(p);self.assertEqual(row['state'],'done')
        files=[self.engine.archive.file(p['snapshot'],i) for i in p['items']]
        for target in ([-1002,7],[OWNER,None]):
            self.store.db.execute('DELETE FROM operations')
            other={**p,'target':target}
            if target==[OWNER,None]:other['items']=[{**i,'caption':'changed'} for i in p['items']]
            await self.engine.send(row,other,other['items'],files)
        self.store.db.execute('DELETE FROM operations')
        other=Engine(self.config,self.store,self.client,self.gate,delivery=self.delivery,bridge=self.bridge,bot_id=123)
        await other.send(row,p,p['items'],files)
        self.assertEqual(self.delivery.send.await_count,4)
        self.store.db.execute('DELETE FROM operations')
        await self.engine.send(row,p,p['items'][:1],files[:1])
        self.assertEqual(self.delivery.send.await_count,5)

    async def test_shared_claim_from_other_process_surfaces_ambiguous(self):
        from tg_forwarder.control.store import encode
        p=await self.payload('forward',[OWNER,None])
        i=p['items'][0]
        op=self.bridge.operation([[str(i['chat']),i['id'],str(i['media_id'])]],OWNER,None,
            'bot:6317800517:control-upload-v1',action='upload',
            caption=encode([[i['id'],i['caption'],i['kind']]]))
        await self.redis.set(self.bridge.prefix+'claim:'+op,'other')
        self.assertEqual((await self.run_task(p))['state'],'ambiguous')
        self.delivery.send.assert_not_awaited()

    async def test_definite_bot_rejection_releases_claim_and_retry_succeeds(self):
        from telegram.error import BadRequest
        p=await self.payload('forward',[OWNER,None])
        self.delivery.send.side_effect=BadRequest('rejected')
        row=await self.run_task(p);self.assertEqual(row['state'],'failed')
        self.assertFalse(any(':claim:' in k for k in self.redis.data))
        self.delivery.send.side_effect=lambda t,g,f:[NS(message_id=777)]
        self.store.set_task(row['id'],state='queued',due=0)
        await self.engine.execute(self.store.claim())
        self.assertEqual(self.store.task(row['id'],OWNER,OWNER)['state'],'done')

    async def test_cancelled_send_retains_shared_claim_and_local_ambiguity(self):
        self.delivery.send.side_effect=asyncio.CancelledError()
        p=await self.payload('forward',[OWNER,None])
        self.assertEqual((await self.run_task(p))['state'],'ambiguous')
        self.assertTrue(any(':claim:' in k for k in self.redis.data))
        self.assertFalse(any(':done:' in k for k in self.redis.data))
        self.assertTrue(next(self.config.root.rglob('*.jpg')).is_file())

    async def test_incomplete_receipt_never_records_done(self):
        self.delivery.send.side_effect=lambda *a:[NS(message_id=None)]
        self.assertEqual((await self.run_task(await self.payload('forward',[OWNER,None])))['state'],'ambiguous')
        self.assertFalse(any(':done:' in k for k in self.redis.data))

    async def test_reference_does_not_copy_or_apply_legacy_blob_size_limit(self):
        self.bridge.max_bytes=3
        row=await self.run_task(await self.payload())
        self.assertEqual(row['state'],'done')
        self.assertFalse(list(self.bridge.root.glob('[0-9a-f]'*64)))
        self.assertEqual(next(self.config.root.rglob('*.jpg')).read_bytes(),b'data')
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM cache').fetchone()[0],1)

    async def test_redis_failure_fail_closed_before_network_download(self):
        self.bridge.cached=AsyncMock(side_effect=ConnectionError('secret redis URL'))
        row=await self.run_task(await self.payload())
        self.assertNotEqual(row['state'],'done');self.assertNotIn('secret',row['error'])
        self.client.main[0].download_media.assert_not_awaited()
        self.delivery.send.assert_not_awaited()

    async def test_composite_rate_delay_persists_and_shared_failure_never_bypasses(self):
        due=self.gate.delay(0)
        await self.gate.wait()
        self.assertGreaterEqual(float(self.store.get_meta('rate_until')),due)
        self.assertTrue(any(k.endswith(':delay') for k in self.redis.data))
        self.bridge.wait=AsyncMock(side_effect=ConnectionError('unavailable'))
        from tg_forwarder.shared_media import SharedUnavailable
        with self.assertRaises(SharedUnavailable):await RateGate(self.store,self.bridge).wait()

    async def test_shared_bad_size_no_local_success(self):
        p=await self.payload();i=p['items'][0]
        source=self.root/'bad';source.write_bytes(b'bad')
        await self.bridge.publish([str(i['chat']),i['id'],str(i['media_id'])],source)
        self.assertEqual((await self.run_task(p))['state'],'failed')
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM cache').fetchone()[0],0)
        self.client.main[0].download_media.assert_not_awaited()

    async def test_runtime_factory_after_secret_file_and_verified_bot_identity(self):
        from tg_forwarder.control import runtime
        secret=self.root/'control.env';secret.write_text('MEDIA_SHARED_ENABLED=true\nTELEGRAM_BOT_TOKEN=fake-token\n')
        client=NS(connect=AsyncMock(),is_user_authorized=AsyncMock(return_value=True),disconnect=AsyncMock())
        bot=NS(get_me=AsyncMock(return_value=NS(id=6317800517)))
        class Context:
            async def __aenter__(self):return bot
            async def __aexit__(self,*args):pass
        async def factory():
            self.assertEqual(os.environ['MEDIA_SHARED_ENABLED'],'true')
            return self.bridge
        engine=NS(worker=AsyncMock())
        controller=NS(watch_loop=AsyncMock(),preview_loop=AsyncMock())
        with patch.dict(os.environ,{'CONTROL_ENV_FILE':str(secret)},clear=True), \
             patch.object(runtime.Config,'from_env',return_value=self.config), \
             patch.object(runtime,'client_from_env',return_value=client), \
             patch.object(runtime,'Bot',return_value=Context()), \
             patch('tg_forwarder.shared_media.configured_bridge',side_effect=factory), \
             patch.object(runtime,'Engine',return_value=engine) as build, \
             patch.object(runtime,'Controller',return_value=controller), \
             patch.object(runtime,'polling',AsyncMock()),patch.object(runtime,'inbox_loop',AsyncMock()), \
             patch.object(runtime,'serve',AsyncMock()):
            await runtime.run()
        self.assertIs(build.call_args.kwargs['bridge'],self.bridge)
        self.assertEqual(build.call_args.kwargs['bot_id'],6317800517)
        self.assertIs(build.call_args.args[3].bridge,self.bridge)
