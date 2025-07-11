#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import os
import sys
from collections import defaultdict
from math import inf
from pathlib import Path

from dfb.dstdb import NoTimestampInNameError
from dfb.dstdb import rpath2apath as _rpath2apath
from dfb.timestamps import timestamp_parser

_r = repr


def _rs(a):
    return repr(str(a))


def rpath2apath(rpath):
    try:
        return _rpath2apath(rpath)
    except (NoTimestampInNameError, ValueError):
        return rpath, None, ""


class DestNotEmptyError(OSError):
    pass


class FileExistsError(DestNotEmptyError):
    pass


def dfblink(
    mount,
    dest,
    before=None,
    after=None,
    maxdepth=None,
    allow_non_empty=False,
    force_overwrite=False,
):
    """
    Build symlinks from 'dest' to rclone 'mount' location.

    All symlinks will be made to be absolute regardless of specification

    Inputs:
    -------
    mount
        Location of rclone mount of the backup. See note below on subdirectory

    dest
        Location to build the links

    before, after [+Inf,-Inf]
        Timestamps for when to show files. Generally speaking 'before' is the
        time at which to create the links. 'after' can be used to isolate specific
        ranges. 'before' defaults to the latest (+Inf) and after defaults to the
        oldest (-Inf)

    maxdepth [None]
        How far to traverse. Default (None) is no limit. The first directory is 0.

    allow_non_empty [False]
        If True, 'dest' must be empty. Otherwise, will fail.

    force_overwrite [False]
        If True, overwrite symlinks that already exists. Otherwise will fail.

    Subdirs Note
    ------------
    The 'mount' location can point to a sub directory of the backup but the rclone
    mount should be at the highest level. This is to allow for reference files to
    correctly resolve.

    For example, if you backups to 'backups:' and want to look at the subdirectory
    'Documents/Pictures', you should

        $ rclone mount backups: mountpoint [flags ...]

    Then

        >>> dfblink("mountpoint/Documents/Pictures",destdir, ...)

    so that references point above 'Documents/Pictures' will resolve.


    """

    if before is None:
        before = inf
    else:
        before = timestamp_parser(before, epoch=True)

    if after is None:
        after = -inf
    else:
        after = timestamp_parser(after, epoch=True)

    logging.logger.debug(f"{before = }, {after = }")

    mount = Path(mount).resolve()
    dest = Path(dest).resolve()
    logging.logger.debug(f"mount = {_rs(mount)}, dest = {_rs(dest)}")

    dest.mkdir(exist_ok=True, parents=True)
    if not allow_non_empty and any(dest.iterdir()):
        raise DestNotEmptyError(f"{_rs(dest)} is not empty. Set allow_non_empty")

    for root, dirs, rpaths in os.walk(mount):
        rel = os.path.relpath(root, mount)
        depth = rel.count("/") + 1 if rel != "." else 0
        if maxdepth is not None and depth > maxdepth:
            logging.logger.debug("max depth hit")
            del dirs[:]  # Do not go deeper
            continue

        dirs[:] = [d for d in dirs if d != ".dfb"]

        # Group by apaths and filter for before
        apaths = defaultdict(list)
        for rpath in rpaths:
            apath, ts, flag = rpath2apath(rpath)

            # filter. Notice both are inclusive.
            if ts is None:
                logging.info(f"file {_rs(rpath)} is missing a timestamp")
                ts = after  # always keep it but give it the lowest value

            if ts > before:
                logging.logger.debug(f"File {_rs(rpath)} is too new. Skipped")
                continue
            if ts < after:
                logging.logger.debug(f"File {_rs(rpath)} is too old. Skipped")
                continue

            apaths[apath].append((ts, rpath, flag))

        for apath, items in apaths.items():
            items.sort()  # Will use ts, the first tuple arg
            ts, rpath, flag = items[-1]

            if flag == "D":
                continue

            src = mount / rel / rpath
            dst = dest / rel / apath

            if flag == "R":
                src0, dst0 = src, dst
                referent = src.read_text().strip()
                try:
                    referent = json.loads(referent)
                except json.JSONDecodeError:
                    referent = {"ver": 1, "path": referent}

                if referent["ver"] == 1:
                    # Take a guess!

                    src = mount / referent["path"]
                    dst = dst.with_suffix(f".WARNING-V1_Ref{dst.suffix}")

                    logging.warn(
                        "Cannot definitively resolve V1 ref for "
                        f"{_rs(src0.relative_to(mount))}. "
                        f"Guessing {_rs(src.relative_to(mount))} "
                        f"which does{'' if src.exists() else ' NOT'} exist. "
                        f"Changing name to {_rs(dst.relative_to(dest))}"
                    )
                elif referent["ver"] == 2:
                    # drop back to os.path for normalization
                    src = Path(os.path.normpath(src.parent / referent["rel"]))
                else:
                    raise ValueError("Unrecognized ref format")

            if dst.exists():
                logging.logger.debug(f"{_rs(dst)} exists")
                if force_overwrite:
                    dst.unlink()
                else:
                    raise FileExistsError(f"{_rs(dst)} exists. Set force_overwrite")

            logging.info(
                f"Linking {_rs(dst.relative_to(dest))} --> {_rs(src.relative_to(mount))}"
            )
            dst.parent.mkdir(exist_ok=True, parents=True)
            os.symlink(src, dst)
