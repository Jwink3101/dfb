#!/usr/bin/env python
import json
import logging

###########################################
import os
import sys
import time
from errno import EACCES
from functools import lru_cache
from os.path import realpath
from threading import Lock, Thread

from .fuse import FUSE, FuseOSError, LoggingMixIn, Operations

p = os.path.abspath(os.path.dirname(__file__))
if p not in sys.path:
    sys.path.insert(0, p)

from collections import defaultdict

from dfb import __version__
from dfb.cli import ISODATEHELP
from dfb.dstdb import NoTimestampInNameError
from dfb.dstdb import rpath2apath as _rpath2apath
from dfb.timestamps import timestamp_parser


def rpath2apath(rpath):
    try:
        return _rpath2apath(rpath)
    except (NoTimestampInNameError, ValueError):
        return rpath, None, ""


REF_IS_V1 = ".REF_IS_V1"


class DFBMount:
    def __init__(
        self,
        ts=None,
        remove_empty=False,
        use_cache=False,
        cache_reset_min=None,
    ):
        self.ts = ts
        self.remove_empty = remove_empty
        if use_cache:
            maxsize = 128
            self.listdir = lru_cache(maxsize=maxsize)(self._listdir)
            self.apath2rpath = lru_cache(maxsize=8 * maxsize)(self._apath2rpath)
            if cache_reset_min:
                Thread(
                    target=self._reset_cache, args=(cache_reset_min,), daemon=True
                ).start()
        else:
            self.listdir = self._listdir
            self.apath2rpath = self._apath2rpath

    def _reset_cache(self, cache_reset_min):
        while True:
            time.sleep(cache_reset_min * 60)
            self.listdir.cache_clear()
            self.apath2rpath.cache_clear()

    def _listdir(self, dirpath, _empty_check=False, return_map=False):
        print(f"_listdir {dirpath = }")
        subdirs = []
        subfiles = []
        for item in os.listdir(dirpath):
            item = os.path.join(dirpath, item)
            {True: subdirs, False: subfiles}[os.path.isdir(item)].append(item)

        items = defaultdict(list)

        for item in subfiles:
            aname, ts, flag = rpath2apath(item)
            if self.ts and ts and ts > self.ts:
                continue

            if flag == "R":
                with open(item, "rt") as fp:
                    referent = fp.read().strip()

                # Handle different versions here
                try:
                    referent = json.loads(referent)
                except json.JSONDecodeError:
                    referent = {"ver": 1, "path": referent}

                ver = referent["ver"]
                if ver == 1:
                    item = item + REF_IS_V1
                    flag = ""
                elif ver == 2:
                    path = os.path.join(os.path.dirname(item), referent["rel"])
                    path = os.path.normpath(path)
                    item = path

            # keep ts so we sort by ts, not item in case of reference
            items[aname].append((ts, item, flag))

        # end for item in subitems

        for val in items.values():
            val.sort()

        items = {k: v[-1] for k, v in items.items()}
        items = {k: rpath for k, (ts, rpath, flag) in items.items() if flag != "D"}

        if _empty_check and items:
            return True

        for item in subdirs:
            if self.remove_empty:
                # Do a recursive call to listdir with _empty_check=True to get
                # results ASAP. (It will return as soon as the file is found)

                try:
                    ssub = self.listdir(item, _empty_check=True)
                except PermissionError:
                    ssub = True  # Assume it's not empty and move on

                if not ssub:  # Empty
                    continue
                elif _empty_check:
                    return True  # do not keep going deeper
            items[item] = item

        if return_map:
            return items
        return list(items)

    def _apath2rpath(self, apath):
        print(f"_apath2rpath {apath = }")
        dirpath = os.path.dirname(apath)
        items = self.listdir(dirpath, return_map=True)
        try:
            return items[apath]
        except KeyError:
            return apath


class DFBLoop(LoggingMixIn, Operations):
    def __init__(
        self,
        root,
        ts=None,
        remove_empty=False,
        use_cache=False,
        cache_reset_min=None,
    ):
        self.dfbmount = DFBMount(
            ts=ts,
            remove_empty=remove_empty,
            use_cache=use_cache,
            cache_reset_min=cache_reset_min,
        )

        self.root = realpath(root)
        self.rwlock = Lock()

    def __call__(self, op, apath, *args):
        # This is *just* a passthrough so keep it as apath!
        return super(DFBLoop, self).__call__(
            op, os.path.join(self.root, apath.removeprefix("/")), *args
        )

    def access(self, apath, mode):
        rpath = self.dfbmount.apath2rpath(apath)
        if not os.access(rpath, mode):
            raise FuseOSError(EACCES)

    #     chmod = os.chmod
    #     chown = os.chown
    #
    #     def create(self, path, mode):
    #         return os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    #
    #     def flush(self, path, fh):
    #         return os.fsync(fh)
    #
    #     def fsync(self, path, datasync, fh):
    #         if datasync != 0:
    #             return os.fdatasync(fh)
    #         else:
    #             return os.fsync(fh)

    def getattr(self, apath, fh=None):
        rpath = self.dfbmount.apath2rpath(apath)
        st = os.lstat(rpath)
        keys = [
            "st_atime",
            "st_ctime",
            "st_gid",
            "st_mode",
            "st_mtime",
            "st_nlink",
            "st_size",
            "st_uid",
        ]
        return {k: getattr(st, k) for k in keys}

    getxattr = None

    #     def link(self, target, source):
    #         return os.link(self.root + source, target)

    listxattr = None

    #     mkdir = os.mkdir
    #     mknod = os.mknod
    #     open = os.open
    def open(self, apath, flags):
        rpath = self.dfbmount.apath2rpath(apath)
        return os.open(rpath, flags)

    def read(self, apath, size, offset, fh):
        rpath = self.dfbmount.apath2rpath(apath)
        with self.rwlock:
            if rpath.endswith(REF_IS_V1):
                rpath = rpath.removesuffix(REF_IS_V1)
                print("WARNING: V1 style reference")
            with open(rpath, "rb") as fp:
                fp.seek(offset)
                return fp.read(size)  #

    #             os.lseek(fh, offset, 0)
    #             return os.read(fh, size)

    def readdir(self, path, fh):
        res = self.dfbmount.listdir(path)
        res = [os.path.basename(r) for r in res]
        res.sort()
        return [".", ".."] + res

    #     readlink = os.readlink

    #     def release(self, path, fh):
    #         return os.close(fh)

    #     def rename(self, old, new):
    #         return os.rename(old, self.root + new)
    #
    #     rmdir = os.rmdir

    def statfs(self, apath):
        rpath = self.dfbmount.apath2rpath(apath)
        stv = os.statvfs(rpath)
        return dict(
            (key, getattr(stv, key))
            for key in (
                "f_bavail",
                "f_bfree",
                "f_blocks",
                "f_bsize",
                "f_favail",
                "f_ffree",
                "f_files",
                "f_flag",
                "f_frsize",
                "f_namemax",
            )
        )


#     def symlink(self, target, source):
#         return os.symlink(source, target)
#
#     def truncate(self, path, length, fh=None):
#         with open(path, 'r+') as f:
#             f.truncate(length)
#
#     unlink = os.unlink
#     utimens = os.utime
#
#     def write(self, path, data, offset, fh):
#         with self.rwlock:
#             os.lseek(fh, offset, 0)
#             return os.write(fh, data)

epilog = """\
Note on Mounting
----------------
This is a thin wrapper around a simple loop-back file system pointed to the dfb
destination. RcloneCLI's mounting capabilities is significantly more powerful so this
lets rclone manage the files and just wraps that mount. This is also read-only so it
makes sense to also have the rclone mount be read-only.

Example: Assume `dst = "backups:server"`

    $ rclone mount backups:server rclone_mount --vfs-cache-mode full --read-only
    $ %(prog)s rclone_mount dfb_mount
   
Note that dfb-mount will work with multiple directories. For example, you have more than
just 'server' on 'backups:', you can rclone mount just 'backups:' then point %(prog)s 
to the top level.
"""

description = """\
dfb FUSE thin-wrapper mount. Must FIRST mount the destination with rclone. Does **not**
work with libfuse3. Must use libfuse2 on Linux
"""


def cli(argv=None):
    import argparse

    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser(
        description=description,
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("rclone_mount_dest", help="Specify where rclone is mounted")
    parser.add_argument("mount_dest", help="Mount dest")
    parser.add_argument(
        "--before",
        "--at",
        help=f"Specify a timestamp. {ISODATEHELP}",
        dest="ts",
    )
    ## TODO: --after flag
    parser.add_argument(
        "--remove-empty-dirs",
        action="store_true",
        help="""
            Try to determine if a directory is empty. Due to dfb keeping all copies
            of files, it is possible to have directories with no active files. This will
            try to determine that. It is disabled by default because of the performance
            hit of having to resurse until files are found""",
    )
    parser.add_argument(
        "--cache",
        action="store_true",
        help="""\
            Whether to cache dir listings. If this is going to be long-running, do not 
            use since new backups won't be updated. But useful for short jobs.
            """,
    )
    parser.add_argument(
        "--cache-reset",
        metavar="minutes",
        type=float,
        help="Specify an interval, in minutes, to reset the cache. If not set, cache will not reset",
    )
    parser.add_argument(
        "--version", action="version", version=f"dfb-mount_dbf.{__version__}"
    )

    args = parser.parse_args(argv)
    dfb = DFBMount()
    # logging.basicConfig(level=logging.DEBUG)
    if args.ts:
        args.ts = timestamp_parser(args.ts, aware=True, epoch=True)
    fuse = FUSE(
        DFBLoop(
            args.rclone_mount_dest,
            ts=args.ts,
            remove_empty=args.remove_empty_dirs,
            use_cache=args.cache,
            cache_reset_min=args.cache_reset,
        ),
        args.mount_dest,
        raw_fi=False,
        foreground=True,
        allow_other=True,
    )
