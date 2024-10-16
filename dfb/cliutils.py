import sys
import logging
import json

from . import apath2rpath, rpath2apath, nowfun
from .utils import timestamp_parser

logger = logging.getLogger(__name__)


def cli_apath2rpath(cliconfig):
    apaths = cliconfig.files

    # This is tested manually
    if apaths.count("-") == 1:  # pragma: no cover
        stdin = sys.stdin.buffer.read()
        stdin = stdin.replace(b"\x00", b"\n")
        stdin = stdin.decode().split("\n")
        stdin = [i for i in stdin if i]

        ix = apaths.index("-")
        apaths = apaths[:ix] + stdin + apaths[ix + 1 :]
    elif apaths.count("-") > 1:
        logger.error("Cannot specify '-' more than once")
        sys.exit(2)

    if cliconfig.date:
        ts = timestamp_parser(cliconfig.date, aware=True)
    else:
        ts = nowfun().obj
    rpaths = [apath2rpath(apath, ts=ts) for apath in apaths]

    sep = b"\x00" if cliconfig.print0 else b"\n"
    out = sep.join(rpath.encode() for rpath in rpaths)

    sys.stdout.buffer.write(out)
    if sys.stdout.isatty():
        sys.stdout.buffer.write(b"\n")


def cli_rpath2apath(cliconfig):
    rpaths = cliconfig.files

    # This is tested manually
    if rpaths.count("-") == 1:  # pragma: no cover
        stdin = sys.stdin.buffer.read()
        stdin = stdin.replace(b"\x00", b"\n")
        stdin = stdin.decode().split("\n")
        stdin = [i for i in stdin if i]

        ix = rpaths.index("-")
        rpaths = rpaths[:ix] + stdin + rpaths[ix + 1 :]
    elif rpaths.count("-") > 1:
        logger.error("Cannot specify '-' more than once")
        sys.exit(2)

    for rpath in rpaths:
        apath, ts, flag = rpath2apath(rpath)
        date = timestamp_parser(ts)
        if cliconfig.timestamp_local:  # pragma: no cover
            date = (
                date.astimezone()
            )  # make it local. Hard to test without knowing timezone

        datestr = date.isoformat()
        res = {"apath": apath, "timestamp": datestr, "flag": flag}

        print(json.dumps(res, indent=None, separators=(",", ":")), flush=True)
