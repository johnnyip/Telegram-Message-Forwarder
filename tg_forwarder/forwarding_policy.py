from dataclasses import dataclass
from typing import Any, Callable, Iterable


def message_or_chat_blocks_forward(info: dict) -> bool:
    return bool(info.get("msg_noforwards")) or bool(info.get("chat_noforwards"))


@dataclass(frozen=True)
class ForwardingPolicyConfig:
    threshold_bytes: int
    policy: str
    allowlist: set[int]
    denylist: set[int]
    enable_direct_forward_jobs: bool = False


def can_attempt_large_forward(chat_id: int, info: dict, *, config: ForwardingPolicyConfig, forward_cache_get: Callable[[int], bool | None]) -> bool:
    if not config.enable_direct_forward_jobs:
        return False

    if chat_id in config.denylist:
        return False

    if message_or_chat_blocks_forward(info):
        return False

    cached = forward_cache_get(chat_id)
    if cached is False:
        return False
    if cached is True:
        return True

    if config.policy == "never":
        return False

    if config.policy == "allowlist":
        return chat_id in config.allowlist

    if config.policy == "auto":
        if config.allowlist and chat_id in config.allowlist:
            return True
        return True

    return False


def should_direct_forward_large_media(info: dict, msg: Any, *, config: ForwardingPolicyConfig, get_media_size: Callable[[Any], int | None], forward_cache_get: Callable[[int], bool | None]) -> bool:
    size = get_media_size(msg)
    if size is None or size <= config.threshold_bytes:
        return False
    return can_attempt_large_forward(info["chat_id"], info, config=config, forward_cache_get=forward_cache_get)


def should_direct_forward_large_album(chat_id: int, info: dict, msgs: Iterable[Any], *, config: ForwardingPolicyConfig, get_media_size: Callable[[Any], int | None], forward_cache_get: Callable[[int], bool | None]) -> bool:
    if not can_attempt_large_forward(chat_id, info, config=config, forward_cache_get=forward_cache_get):
        return False

    for msg in msgs:
        size = get_media_size(msg)
        if size is not None and size > config.threshold_bytes:
            return True
    return False
