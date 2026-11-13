"""Execute pure helpers and actual main startup branches without importing startup side effects."""
import ast
import asyncio
import os
from pathlib import Path
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock, Mock, patch
from telegram.error import TelegramError


def startup_code():
    tree=ast.parse(Path('main.py').read_text())
    names={'authenticated_delivery_identity','bind_authenticated_delivery_identity','main'}
    nodes=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name in names]
    main=next(n for n in nodes if n.name=='main')
    # Isolate startup from unrelated shutdown flushing; startup body is unchanged.
    outer=next(n for n in main.body if isinstance(n,ast.Try))
    outer.finalbody=[ast.copy_location(ast.Pass(),outer)]
    return compile(ast.fix_missing_locations(ast.Module(body=nodes,type_ignores=[])),'main.py','exec')


class StartupIdentityTests(unittest.IsolatedAsyncioTestCase):
    def context(self, mode, principal=77, error=None):
        class Client:
            get_me=AsyncMock(return_value=NS(id=principal,bot=False),side_effect=error)
            run_until_disconnected=AsyncMock()
            async def __aenter__(self):return self
            async def __aexit__(self,*args):return False
        env={'os':os,'asyncio':asyncio,'APP_MODE':mode,'client':Client(),
             'bot':NS(get_me=AsyncMock(return_value=NS(id=principal),side_effect=error)),
             'DOWNLOAD_DIR':NS(mkdir=Mock()),'LOG_DIR':NS(mkdir=Mock()),'SPOOL_DIR':NS(mkdir=Mock()),'RETAIN_DIR':NS(mkdir=Mock()),
             'cleanup_logs':Mock(),'cleanup_retained_files':Mock(),'print_route_summary':Mock(),'touch_health':Mock(),
             'startup_banner':lambda:'startup','print':Mock(),'LOGGER':NS(info=Mock(),warning=Mock()),'log':Mock(),
             'register_telethon_handlers':Mock(),'startup_kafka_producer':AsyncMock(),'startup_kafka_consumers':AsyncMock(),
             'TELEGRAM_BOT_TOKEN':'fake-never-used','BOT_STARTUP_SMOKE_TEST':False,'DATE_SEPARATOR_ENABLED':False,
             'text_consumer_loop':AsyncMock(),'media_consumer_loop':AsyncMock(),'TelegramError':TelegramError}
        exec(startup_code(),env)
        return env

    async def test_pure_authenticated_defaults_suffix_preservation_and_spoof_rejection(self):
        identity=self.context('listen')['authenticated_delivery_identity']
        self.assertEqual(identity('user',77),'user:77:native-v1')
        self.assertEqual(identity('bot',88),'bot:88:legacy-mirror-v1')
        self.assertEqual(identity('bot',88,'bot:88:custom-v2'),'bot:88:custom-v2')
        for configured in ('bot:89:custom','user:88:custom','bot:88:','bot:88: custom'):
            with self.assertRaises(ValueError):identity('bot',88,configured)
        for principal in (None,True,0,-1,'77'):
            with self.assertRaises(ValueError):identity('user',principal)

    async def test_user_binding_precedes_producer_and_event_handlers(self):
        env=self.context('listen')
        ordering=[]
        async def producer():
            self.assertEqual(os.environ['MEDIA_NATIVE_DELIVERY_IDENTITY'],'user:77:native-v1')
            env['register_telethon_handlers'].assert_not_called()
            ordering.append('producer')
        env['startup_kafka_producer']=producer
        env['register_telethon_handlers'].side_effect=lambda:ordering.append('handlers')
        with patch.dict(os.environ,{'MEDIA_SHARED_ENABLED':'true'},clear=True):await env['main']()
        self.assertEqual(ordering,['producer','handlers'])

    async def test_bot_binding_precedes_consumers_and_preserves_matching_semantics(self):
        env=self.context('send',principal=88)
        async def consumers():self.assertEqual(os.environ['MEDIA_DELIVERY_IDENTITY'],'bot:88:custom-v3')
        env['startup_kafka_consumers']=consumers
        with patch.dict(os.environ,{'MEDIA_SHARED_ENABLED':'true','MEDIA_DELIVERY_IDENTITY':'bot:88:custom-v3'},clear=True):await env['main']()
        env['text_consumer_loop'].assert_awaited_once()
        # Missing explicit identity derives from the authenticated bot, never target owner.
        env=self.context('send',principal=88)
        async def defaults():self.assertEqual(os.environ['MEDIA_DELIVERY_IDENTITY'],'bot:88:legacy-mirror-v1')
        env['startup_kafka_consumers']=defaults
        with patch.dict(os.environ,{'MEDIA_SHARED_ENABLED':'true'},clear=True):await env['main']()

    async def test_shared_probe_failure_or_spoof_stops_before_work_starts(self):
        for mode,error in [('listen',OSError('auth failed')),('send',TelegramError('auth failed'))]:
            env=self.context(mode,error=error)
            with patch.dict(os.environ,{'MEDIA_SHARED_ENABLED':'true'},clear=True),self.assertRaises(type(error)):
                await env['main']()
            env['startup_kafka_producer'].assert_not_awaited();env['startup_kafka_consumers'].assert_not_awaited()
            env['register_telethon_handlers'].assert_not_called()
        for mode,name,spoof in [('listen','MEDIA_NATIVE_DELIVERY_IDENTITY','user:192114587:native-v1'),('send','MEDIA_DELIVERY_IDENTITY','bot:192114587:legacy-mirror-v1')]:
            env=self.context(mode)
            with patch.dict(os.environ,{'MEDIA_SHARED_ENABLED':'true',name:spoof},clear=True),self.assertRaises(ValueError):await env['main']()
            env['startup_kafka_producer'].assert_not_awaited();env['startup_kafka_consumers'].assert_not_awaited()

    async def test_default_off_preserves_warning_continue_and_does_not_set_principals(self):
        for mode,error in [('listen',OSError('auth failed')),('send',TelegramError('auth failed'))]:
            env=self.context(mode,error=error)
            with patch.dict(os.environ,{},clear=True):
                await env['main']()
                self.assertNotIn('MEDIA_NATIVE_DELIVERY_IDENTITY',os.environ)
                self.assertNotIn('MEDIA_DELIVERY_IDENTITY',os.environ)
            if mode=='listen':env['startup_kafka_producer'].assert_awaited_once()
            else:env['startup_kafka_consumers'].assert_awaited_once()

if __name__=='__main__':unittest.main()
