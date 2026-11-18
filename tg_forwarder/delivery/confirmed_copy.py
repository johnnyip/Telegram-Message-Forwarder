"""Per-copy permanent receipts. Never retry an uncertain HTTP side effect.

Legacy parent receipts remain compatible; these child receipts protect a successful
forum copy when a later general/other-target transport is definitely rejected.
"""
from types import SimpleNamespace
from ..shared_media import configured_bridge, AmbiguousDelivery
import os

async def confirmed_copy(send, *, info, target, thread, caption, album=False):
    identities = info.get("_delivery_identities")
    bridge = await configured_bridge()
    if bridge is None:
        return await send()
    if not identities or any(i is None for i in identities):
        # Legacy callers without snapshots retain their parent's conservative fence.
        return await send()
    operation = bridge.operation(identities, target, {"copy_v1":thread},
        os.environ.get("MEDIA_DELIVERY_IDENTITY"), action="bot_album_copy" if album else "bot_file_copy", caption=caption)
    async def transport():
        result = await send()
        values = list(result) if album else [result]
        ids = [getattr(v,"message_id",None) for v in values]
        expected = len(identities) if album else 1
        if len(ids)!=expected or len(set(ids))!=expected or any(type(i) is not int or i<=0 for i in ids):
            raise AmbiguousDelivery("Incomplete per-copy Telegram receipt; reconcile before retry")
        return SimpleNamespace(sent_message_id=ids[0], sent_message_ids=ids, delivery_kind="copy")
    receipt = await bridge.deliver(operation, transport)
    if album:
        return [SimpleNamespace(message_id=i) for i in receipt.sent_message_ids]
    return SimpleNamespace(message_id=receipt.sent_message_id)
