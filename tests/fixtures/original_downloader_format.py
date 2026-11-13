"""MIT excerpt: tangyoha/telegram_media_downloader fe7ccdc2cc5fce5b04a464bc66cfb1266bf7ef45 utils/format.py. Copyright (c) 2019 Dineshkarthik R. See docs/licenses/tangyoha-downloader-MIT.txt."""
import os
import re
import unicodedata

def truncate_filename(path: str, limit: int = 230) -> str:
    """Truncate filename to the max len.

    Parameters
    ----------
    path: str
        File name path

    limit: int
        limit file name len(utf-8 byte)

    Returns
    -------
    str
        if file name len more than limit then return truncate filename or return filename

    """
    p, f = os.path.split(os.path.normpath(path))
    f, e = os.path.splitext(f)
    f_max = limit - len(e.encode("utf-8"))
    f = unicodedata.normalize("NFC", f)
    f_trunc = f.encode()[:f_max].decode("utf-8", errors="ignore")
    return os.path.join(p, f_trunc + e)


def validate_title(title: str) -> str:
    """Fix if title validation fails

    Parameters
    ----------
    title: str
        Chat title

    """

    r_str = r"[/\\:*?\"<>|\n]"  # '/ \ : * ? " < > |'
    new_title = re.sub(r_str, "_", title)
    return new_title
