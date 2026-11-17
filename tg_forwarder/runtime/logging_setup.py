import logging
import os
import re

_TOKEN = re.compile(r"(?i)(?:bot)?[0-9]{6,}:[A-Za-z0-9_-]{20,}")

def redact(value):
    value = _TOKEN.sub('[BOT_TOKEN_REDACTED]', str(value))
    for name, secret in os.environ.items():
        if any(word in name.upper() for word in ('TOKEN', 'PASSWORD', 'API_HASH', 'SESSION_STRING')) and len(secret) >= 8:
            value = value.replace(secret, '[SECRET_REDACTED]')
    return value

class SafeFormatter(logging.Formatter):
    def format(self, record):
        return redact(super().format(record))

def setup_logging():
    level = getattr(logging, os.getenv('LOG_LEVEL', 'INFO').upper(), logging.INFO)
    logging.basicConfig(level=level)
    for handler in logging.getLogger().handlers:
        handler.setFormatter(SafeFormatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('httpcore').setLevel(logging.WARNING)
    return logging.getLogger('tg_forwarder')

__all__ = ['setup_logging', 'redact', 'SafeFormatter']
