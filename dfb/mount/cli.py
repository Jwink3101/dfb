import argparse
import logging
import os
import sys
from errno import EACCES, ENOENT
from threading import Lock

from .. import __version__
from ..cli import ISODATEHELP
from ..timestamps import timestamp_parser
from .view import DFBMountView

logger = logging.getLogger(__name__)


epilog = """\
Note on Mounting
----------------
This is a thin wrapper around a simple loop-back file system pointed to the dfb
destination. Rclone's mounting capabilities are more powerful, so this overlays
an rclone mount and presents the dfb view.

Example: Assume `dst = "backups:server"`

    $ rclone mount backups:server rclone_mount --vfs-cache-mode full --read-only
    $ %(prog)s rclone_mount dfb_mount

You can mount a top-level remote and point dfb-mount at any subdirectory.
"""

description = """\
dfb FUSE thin-wrapper mount. Must FIRST mount the destination with rclone. This
implementation relies on mfusepy (external) rather than a bundled FUSE shim.
"""


class DFBLoop:
    def __init__(self, view, fuse_error):
        self.view = view
        self._fuse_error = fuse_error
        self._rwlock = Lock()
        self._del_text = self.view.delete_suffix().strip()

    def access(self, apath, mode):
        rpath = self.view.apath2rpath(apath)
        if os.access(rpath, mode):
            return 0
        # Avoid noisy permission errors from OS probes; open() will enforce access.
        return 0

    def getattr(self, apath, fh=None):
        rpath = self.view.apath2rpath(apath)
        try:
            st = os.lstat(rpath)
        except FileNotFoundError:
            raise self._fuse_error(ENOENT)
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
        info = {k: getattr(st, k) for k in keys}
        if self.view.is_deleted_view(apath):
            info["st_size"] = len(self._del_text)
        return info

    getxattr = None
    listxattr = None

    def open(self, apath, flags):
        rpath = self.view.apath2rpath(apath)
        return os.open(rpath, flags)

    def read(self, apath, size, offset, fh):
        if self.view.is_deleted_view(apath):
            data = self._del_text.encode("utf-8")
            return data[offset : offset + size]
        with self._rwlock:
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def readdir(self, apath, fh):
        try:
            res = self.view.listdir(apath)
        except FileNotFoundError:
            raise self._fuse_error(ENOENT)
        names = [os.path.basename(r) for r in res]
        names.sort()
        return [".", ".."] + names

    def release(self, apath, fh):
        return os.close(fh)

    def statfs(self, apath):
        rpath = self.view.apath2rpath(apath)
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


def _load_fuse(fuse_library=None):
    if fuse_library:
        os.environ["FUSE_LIBRARY_PATH"] = fuse_library
    try:
        from mfusepy import FUSE, FuseOSError
    except Exception as exc:
        raise SystemExit(
            "Mounting requires mfusepy. Install with: "
            "python -m pip install dfb[mount] or python -m pip install mfusepy"
        ) from exc
    logger.debug(
        "Loaded mfusepy (FUSE_LIBRARY_PATH=%s)", os.environ.get("FUSE_LIBRARY_PATH")
    )
    return FUSE, FuseOSError


def _add_mount_args(parser):
    parser.add_argument("rclone_mount_loc", help="Path where rclone is mounted")
    parser.add_argument("mount_dest", help="Mount dest")
    parser.add_argument(
        "--before",
        "--at",
        help=f"Specify a timestamp. {ISODATEHELP}",
        dest="ts",
    )
    parser.add_argument(
        "--after",
        help=f"Only show files after the specified time. {ISODATEHELP}",
    )
    empty_group = parser.add_argument_group("Empty Directory Handling")
    empty_group.add_argument(
        "--remove-empty-dirs",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="""\
            Try to determine if a directory is empty and hide those with no visible files.
            Due to dfb keeping all copies of files, directories can remain after deletes.
            This may walk subdirectories and can be expensive. Default: disabled
            (--no-remove-empty-dirs).
            """,
    )
    empty_group.add_argument(
        "--empty-check-depth",
        type=int,
        metavar="N",
        help="""\
            When --remove-empty-dirs is enabled, limit how deep to scan for visible files.
            Depth 0 checks only the directory itself, depth 1 checks one level below, and
            so on. Lower depths are faster but less certain. Ignored unless
            --remove-empty-dirs is set.
            """,
    )
    empty_group.add_argument(
        "--empty-check-fail",
        choices=("hide", "show"),
        default="hide",
        help="""\
            Behavior when --empty-check-depth cannot determine emptiness because the depth
            limit is reached before a visible file is found. 'hide' treats it as empty and
            hides the directory; 'show' keeps it visible. Default: hide.
            """,
    )
    parser.add_argument(
        "--undated-files",
        choices=("show", "hide", "tag"),
        default="tag",
        help="""\
            How to handle files without a datetag. 'show' keeps them as normal files,
            'hide' excludes them, and 'tag' shows them with a trailing [NODATE]. Empty
            directory checks use this same visibility behavior. Default: tag.
            """,
    )
    parser.add_argument(
        "--cache",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="""\
            Cache directory listings. Defaults to enabled when --before/--at is used,
            otherwise disabled. For long-running mounts, consider --no-cache so new
            backups are visible.
            """,
    )
    parser.add_argument(
        "--cache-reset",
        metavar="minutes",
        type=float,
        help="Specify an interval, in minutes, to reset the cache. If not set, cache will not reset",
    )
    parser.add_argument(
        "--fuse-library",
        metavar="path",
        help="Optional libfuse path/name (e.g. fuse-t) for macOS FUSE-T setups.",
    )
    parser.add_argument(
        "--show-deleted",
        action="store_true",
        help="""\
            Show deleted files with a trailing [DEL] suffix. Opening these files returns
            literal [DEL] content. Note that directories can still appear empty if you are
            viewing an earlier --before/--at time and no files existed yet.
            """,
    )
    log_group = parser.add_mutually_exclusive_group()
    log_group.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Only show warnings and errors.",
    )
    log_group.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    parser.add_argument(
        "--version", action="version", version=f"dfb-mount.{__version__}"
    )

    return parser


def add_parser(subparsers):
    parser = subparsers.add_parser(
        "mount",
        description=description,
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help="Mount a dfb view over an rclone mount (optional dependency).",
    )
    return _add_mount_args(parser)


def run(args):
    if args.ts:
        args.ts = timestamp_parser(args.ts, aware=True, epoch=True)
    if args.after:
        args.after = timestamp_parser(args.after, aware=True, epoch=True)

    FUSE, FuseOSError = _load_fuse(args.fuse_library)

    use_cache = args.cache
    if use_cache is None:
        use_cache = args.ts is not None

    view = DFBMountView(
        args.rclone_mount_loc,
        ts=args.ts,
        after=args.after,
        remove_empty=args.remove_empty_dirs,
        empty_check_depth=args.empty_check_depth,
        empty_check_fail=args.empty_check_fail,
        use_cache=use_cache,
        cache_reset_min=args.cache_reset,
        show_deleted=args.show_deleted,
        undated_files=args.undated_files,
    )

    log_level = logging.INFO
    if args.quiet:
        log_level = logging.WARNING
    elif args.verbose:
        log_level = logging.DEBUG
    logging.basicConfig(level=log_level)
    logger.info(
        "Mounting dfb view from %s to %s", args.rclone_mount_loc, args.mount_dest
    )

    FUSE(
        DFBLoop(view, FuseOSError),
        args.mount_dest,
        raw_fi=False,
        foreground=True,
        allow_other=True,
    )


def cli(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser(
        description=description,
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _add_mount_args(parser)

    args = parser.parse_args(argv)
    return run(args)
