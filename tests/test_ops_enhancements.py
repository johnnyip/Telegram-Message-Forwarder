import asyncio
import io
import json
import logging
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock,patch
from tg_forwarder.runtime.logging_setup import redact,SafeFormatter
from tg_forwarder.delivery import pending_edits as pe,edit_updates as eu
from tg_forwarder import spool_lifecycle as sl

class LoggingTests(unittest.TestCase):
    def test_token_and_env_secrets(self):
        token='123456789:FAKE_'+'x'*35
        with patch.dict(os.environ,{'EXAMPLE_PASSWORD':'another-test-secret'}):
            result=redact('https://api.telegram.org/bot'+token+'/sendPhoto another-test-secret')
        self.assertNotIn(token,result);self.assertNotIn('another-test-secret',result)
    def test_exception_and_arguments_redacted(self):
        stream=io.StringIO();h=logging.StreamHandler(stream);h.setFormatter(SafeFormatter('%(message)s'))
        logger=logging.Logger('test');logger.addHandler(h)
        token='123456789:FAKE_'+'z'*35
        try:raise ValueError(token)
        except ValueError:logger.exception('request %s',token)
        self.assertNotIn(token,stream.getvalue());self.assertIn('REDACTED',stream.getvalue())

class QueueTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)/'spool';self.root.mkdir();sl.configure(self.root)
        self.addCleanup(sl._MANAGERS.pop,self.root,None)
        self.routes=[{'name':'r','sources':{1},'targets':[7,8]}]
        self.sig=pe.route_signature(self.routes,1)
        self.info={'chat_id':1,'msg_id':2,'edit_date':'2026-11-17T14:00:00+00:00'}
    def test_durable_deduplicated_and_expires(self):
        first=pe.enqueue(self.info,'new','caption',self.sig,now=100)
        self.assertEqual(first,pe.enqueue(self.info,'new','caption',self.sig,now=101))
        sl._MANAGERS.pop(self.root,None)
        rows,expired=pe.pending(now=102);self.assertEqual(len(rows),1);self.assertEqual(expired,0)
        self.assertEqual(pe.pending(now=100+86401),( [],1))
    async def test_no_mapping_is_retained_without_waiting(self):
        pe.enqueue(self.info,'new','caption',self.sig)
        with patch.object(eu,'get_message_mapping',AsyncMock(return_value={})):
            await asyncio.wait_for(pe.replay_once(None,lambda _:None,self.routes,None),2)
        with sl.manager().db() as c:self.assertEqual(c.execute('SELECT state FROM pending_edits').fetchone()[0],'pending')
    async def test_changed_route_does_not_transport(self):
        pe.enqueue(self.info,'new','caption',self.sig)
        send=AsyncMock()
        with patch.object(pe,'edit_forwarded_media_caption',send):
            await pe.replay_once(None,lambda _:None,[],None)
        send.assert_not_awaited()
        with sl.manager().db() as c:self.assertEqual(c.execute('SELECT state FROM pending_edits').fetchone()[0],'policy_changed')
    async def test_success_kept_for_late_target_mapping(self):
        identity=pe.enqueue(self.info,'new','caption',self.sig)
        send=AsyncMock(return_value=True)
        with patch.object(eu,'get_message_mapping',AsyncMock(return_value={'targets':{'7':{}}})),patch.object(pe,'edit_forwarded_media_caption',send):
            await pe.replay_once(None,lambda _:None,self.routes,None)
        with sl.manager().db() as c:
            self.assertEqual(c.execute('SELECT state FROM pending_edits').fetchone()[0],'pending')
            c.execute('UPDATE pending_edits SET due=0')
        with patch.object(eu,'get_message_mapping',AsyncMock(return_value={'targets':{'7':{},'8':{}}})),patch.object(pe,'edit_forwarded_media_caption',send):
            await pe.replay_once(None,lambda _:None,self.routes,None)
        self.assertEqual(send.await_count,2)
    async def test_obsolete_destination_fails_closed(self):
        pe.enqueue(self.info,'new','caption',self.sig);send=AsyncMock()
        with patch.object(eu,'get_message_mapping',AsyncMock(return_value={'targets':{'99':{}}})),patch.object(pe,'edit_forwarded_media_caption',send):
            await pe.replay_once(None,lambda _:None,self.routes,None)
        send.assert_not_awaited()
    def test_source_locks_do_not_serialize_other_sources(self):
        one=eu.history_lock(self.info,False);two=eu.history_lock({**self.info,'chat_id':3},False)
        self.assertIsNot(one,two);self.assertIs(one,eu.history_lock(self.info,False))
    async def test_gc_backlog_short_delay(self):
        m=SimpleNamespace(scan=lambda:{'owner':True,'complete':False,'deleted':1})
        with patch.object(sl,'manager',return_value=m),patch.object(sl.asyncio,'sleep',AsyncMock(side_effect=asyncio.CancelledError)) as sleep:
            with self.assertRaises(asyncio.CancelledError):await sl.gc_loop(lambda _:None)
        sleep.assert_awaited_once_with(2)

class ControlHealthTests(unittest.TestCase):
    def test_socket_health_does_not_claim_readiness_without_poll(self):
        from tg_forwarder.control import runtime
        db=sqlite3.connect(':memory:');self.addCleanup(db.close)
        db.executescript('CREATE TABLE tasks(state);CREATE TABLE updates(state);')
        with patch.dict(runtime._HEALTH,{'started':time.time(),'poll_success':None,'poll_errors':3,'poll_error_class':'TimedOut','inbox_tick':time.time()}):
            result=runtime.health_snapshot(SimpleNamespace(db=db))
            self.assertEqual(result['status'],'ok');self.assertFalse(result['ready']);self.assertEqual(result['poll_errors'],3)
    def test_recent_poll_ready(self):
        from tg_forwarder.control import runtime
        db=sqlite3.connect(':memory:');self.addCleanup(db.close)
        db.executescript('CREATE TABLE tasks(state);CREATE TABLE updates(state);')
        with patch.dict(runtime._HEALTH,{'poll_success':time.time()}):
            self.assertTrue(runtime.health_snapshot(SimpleNamespace(db=db))['ready'])

class AckTests(unittest.TestCase):
    def test_all_expected_copies_must_acknowledge(self):
        event={'routes':[['r',[7,8]]],'text':'v2','info':{'edit_date':'2026-11-17T14:00:00+00:00'}}
        payload={'general_message_id':1,'topic_message_id':2,'edit_progress':{'1':{'source_text':'v2'}}}
        mapping={'targets':{'7':payload,'8':payload}}
        self.assertFalse(pe.acknowledged(mapping,event))
        payload['edit_progress']['2']={'source_text':'v2'}
        self.assertTrue(pe.acknowledged(mapping,event))
        del mapping['targets']['8'];self.assertFalse(pe.acknowledged(mapping,event))
    def test_newer_applied_revision_supersedes_old_queue(self):
        event={'routes':[['r',[7]]],'text':'v2','info':{'edit_date':'2026-11-17T14:00:00+00:00'}}
        payload={'message_id':1,'edit_progress':{'1':{'source_text':'v3','edit_date':'2026-11-17T14:01:00+00:00'}}}
        self.assertTrue(pe.acknowledged({'targets':{'7':payload}},event))

class TopicTests(unittest.IsolatedAsyncioTestCase):
    async def test_nonforum_negative_cache_and_refresh(self):
        import ast
        from typing import List,Dict,Any
        tree=ast.parse(Path('main.py').read_text())
        node=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='_enrich_topic_hints')
        class ChannelForumMissingError(Exception):pass
        discovery=AsyncMock(side_effect=ChannelForumMissingError())
        clock=[100]
        env={'List':List,'Dict':Dict,'Any':Any,'client':object(),'get_topic_mapping':AsyncMock(return_value={}),
             'find_latest_topic_for_sender':discovery,'store_topic_mapping':AsyncMock(),'_NONFORUM_TARGETS':{},
             'now_ts':lambda:clock[0],'tstamp':lambda:'now','log':lambda _:None}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'main.py','exec'),env)
        fn=env['_enrich_topic_hints'];routes=[{'targets':[7]}]
        await fn({'sender_id':1},routes);await fn({'sender_id':2},routes)
        self.assertEqual(discovery.await_count,1)
        clock[0]+=901;await fn({'sender_id':3},routes)
        self.assertEqual(discovery.await_count,2)
