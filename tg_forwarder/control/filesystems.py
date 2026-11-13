"""Linux mountinfo guards, before mkdir or SQLite/archive writes."""
from pathlib import Path
import re
import sys


class FilesystemError(RuntimeError):
    pass


NETWORK = {"nfs", "nfs4", "cifs", "smbfs", "smb3", "9p", "ceph", "glusterfs",
           "fuse.sshfs", "sshfs", "fuse.glusterfs", "afs", "autofs"}


def unescape(value):
    return re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1],8)), value)


def mounts():
    if sys.platform != "linux":
        raise FilesystemError("Filesystem guards require Linux mountinfo")
    try:
        rows = []
        for line in Path("/proc/self/mountinfo").read_text().splitlines():
            left, right = line.split(" - ",1)
            fields, filesystem = left.split(), right.split()
            rows.append((Path(unescape(fields[4])), filesystem[0]))
        if not rows:
            raise ValueError()
        return rows
    except (OSError, ValueError, IndexError):
        raise FilesystemError("Linux mountinfo unavailable or malformed") from None


def covering(path, rows):
    path = Path(path).absolute()
    matches = [(mount,kind) for mount,kind in rows if path.is_relative_to(mount)]
    if not matches:
        raise FilesystemError("Cannot identify backing filesystem")
    return max(enumerate(matches),key=lambda entry:(len(entry[1][0].parts),entry[0]))[1]


def guard_state(path):
    path = Path(path).absolute()
    for component in (path,*path.parents):
        if component.is_symlink():
            raise FilesystemError("SQLite state symlink rejected")
    _, kind = covering(path,mounts())
    if kind in NETWORK or kind.startswith("fuse."):
        raise FilesystemError("SQLite control state must be on a local filesystem, not network storage")


def guard_archive(root, expected=None, fstypes=("nfs","nfs4")):
    if expected is None:
        return
    root, expected = Path(root).absolute(), Path(expected).absolute()
    if not root.is_relative_to(expected):
        raise FilesystemError("Archive directory is outside expected archive mount")
    rows = mounts()
    exact = [kind for mount,kind in rows if mount == expected]
    if not exact or exact[-1] not in fstypes or covering(root,rows)[1] not in fstypes:
        raise FilesystemError("Expected archive mount missing or filesystem type mismatch; refusing local fallback")


def validate(config):
    guard_state(config.db)
    guard_archive(config.root,config.expected_archive_mount,config.expected_archive_fstypes)
