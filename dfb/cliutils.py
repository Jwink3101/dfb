import json
import logging
import sys

from . import apath2rpath, nowfun, rpath2apath
from .timestamps import timestamp_parser
from .utils import format_timestamp_tag, time2all, timestamp_to_epoch

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
        ts = timestamp_to_epoch(cliconfig.date)
    else:
        ts = nowfun().ts
    rpaths = [apath2rpath(apath, ts=ts, fmt=cliconfig.format) for apath in apaths]

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


def cli_tsconv(cliconfig):
    for raw in cliconfig.values:
        ts = timestamp_to_epoch(raw)
        allts = time2all(ts)
        res = {
            "input": raw,
            "timestamp": timestamp_parser(ts).isoformat(),
            "datetag": allts.dt,
            "base36_epoch": format_timestamp_tag(ts, fmt="base36-epoch"),
        }

        if cliconfig.raw:
            print(
                res["datetag"] if cliconfig.to == "datetag" else res["base36_epoch"],
                flush=True,
            )
            continue

        print(json.dumps(res, indent=None, separators=(",", ":")), flush=True)
