"""
Utilities
"""

import datetime
import logging
import mimetypes
import os
import random
import re
import shlex
import sqlite3
import subprocess
import sys
from collections import namedtuple

from .timestamps import iso8601_parser, timestamp_parser

logger = logging.getLogger(__name__)

tsrep = namedtuple("timestamps", ("ts", "dt", "obj", "pretty"))
BASE36_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz"
BASE36_EPOCH_WIDTH = 8
BASE36_EPOCH_MAX = 36**BASE36_EPOCH_WIDTH - 1
TIMESTAMP_FORMATS = {"datetag", "base36-epoch"}


class NoTimestampInNameError(ValueError):
    pass


def time2all(dt_or_ts):
    """Convert from dt or ts to all formats"""
    if isinstance(dt_or_ts, str):
        n = len(dt_or_ts)
        if n == 14:  # No timezone. Assume UTC
            dt_or_ts += "Z"
        elif n < 11:
            try:
                dt_or_ts = int(dt_or_ts)
            except:
                pass
    obj = timestamp_parser(dt_or_ts, utc=True)
    dt = obj.astimezone(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")
    ts = int(obj.timestamp())
    pretty = obj.astimezone().isoformat()
    return tsrep(ts, dt, obj, pretty)


class MyRow(sqlite3.Row):
    """Fancier but performant sqlite3 row"""

    def todict(self):
        return {k: self[k] for k in self.keys()}

    def values(self):
        for k in self.keys():
            yield self[k]

    def items(self):
        for k in self.keys():
            yield k, self[k]

    def get(self, key, default=None):
        try:
            return self[key]
        except:
            return default

    def __str__(self):
        return "Row(" + str(self.todict()) + ")"

    __repr__ = __str__


class star:
    """
    Wrapper to make an iterable of arguments be applied as positional.

        star(fun)(args) --> fun(*args)

    Also passes additional args

        star(fun)(args,other1,other2,kw=value) --> fun(*args,other1,other2,kw=value)
    """

    def __init__(self, fun):
        self.fun = fun

    def __call__(self, arg, *args, **kwargs):
        return self.fun(*arg, *args, **kwargs)


def tabulate(table, indent=2, sep="  "):
    """Fancy printing of data"""
    tabulated = []
    nc = [len(c) for c in table[0]]
    for row in table[1:]:
        for ic, c in enumerate(row):
            nc[ic] = max(nc[ic], len(c))

    for row in table:
        r = [f"{c:>{n}s}" for c, n in zip(row[:-1], nc[:-1])]
        r.append(f"{row[-1]:<{nc[-1]}s}")
        r = " " * indent + sep.join(r).rstrip()
        tabulated.append(r)
    return "\n".join(tabulated)


def human_readable_bytes(
    byte_count,
    base=int(os.environ.get("DFB_BASE", 1024)),  # undocumented environment setting
    short=True,
    fmt=False,
):
    """
    Return a value,label tuple with human readable sizes or,
    if fmt = True, return a formatted version as `{value:0.2f} {label:s}`

    |        Decimal            |            Binary       |
    | 1000    | kB | kilobyte   | 1024     KiB | kibibyte |
    | 1000^2  | MB | megabyte   | 1024^2   MiB | mebibyte |
    | 1000^3  | GB | gigabyte   | 1024^3   GiB | gibibyte |
    | 1000^4  | TB | terabyte   | 1024^4   TiB | tebibyte |
    | 1000^5  | PB | petabyte   | 1024^5   PiB | pebibyte |
    | 1000^6  | EB | exabyte    | 1024^6   EiB | exbibyte |
    | 1000^7  | ZB | zettabyte  | 1024^7   ZiB | zebibyte |
    | 1000^8  | YB | yottabyte  | 1024^8   YiB | yobibyte |

    Example:
        >>> human_readable_bytes(2580417210)
            (2.40320079959929, 'gb')
    """
    if base not in (1024, 1000):
        raise ValueError("base must be 1000 or 1024")

    best = 0
    for ii in range(9):
        if (byte_count / (base**ii * 1.0)) < 1:
            break
        best = ii

    if base == 1000 and short:
        labels = ["B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
    elif base == 1000 and not short:
        labels = ["", "kilo", "mega", "giga", "tera", "peta", "exa", "zetta", "yotta"]
        labels = [l + "byte" for l in labels]
    elif base == 1024 and short:
        labels = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"]
    elif base == 1024 and not short:
        labels = ["", "ki", "me", "gi", "te", "pe", "ex", "ze", "yo"]
        labels = [l + "bibyte" for l in labels]

    res = byte_count / (base**best * 1.0), labels[best]
    if fmt:
        return "{0:g} {1:s}".format(*res)
    return res


def parse_bytes(strsize):
    if isinstance(strsize, (int, float)):
        return int(strsize)

    prefix2bytes = {"b": 1}

    dec_prefix = ["", "kilo", "mega", "giga", "tera", "peta", "exa", "zetta", "yotta"]
    bin_prefix = ["", "kibi", "mebi", "gibi", "tebi", "pebi", "exbi", "zebi", "yobi"]

    for ii, (dp, bp) in enumerate(zip(dec_prefix, bin_prefix)):
        prefix2bytes[dp] = 1000**ii
        prefix2bytes[bp] = 1024**ii

        if ii:
            c = dp[0]
            prefix2bytes[c] = prefix2bytes[f"{c}b"] = 1000**ii
            prefix2bytes[f"{c}i"] = prefix2bytes[f"{c}ib"] = 1024**ii

    strsize = (
        strsize.lower()
        .replace("bytes", "")
        .replace("byte", "")
        .replace(" ", "")
        .strip()
    )

    match = re.match(r"([\d|\.]+)(\D*)", strsize)
    if not match:
        raise ValueErorr("Could not parse")
    val, units = match.groups()
    val = float(val)
    try:
        uval = prefix2bytes[units]
    except KeyError:
        raise ValueError(f"Unrecognized {units = }")

    return int(val * uval)


def shell_runner(cmds, dry=False, env=None, prefix=""):
    """
    Run the shell command (string or list) and return the returncode
    """

    environ = os.environ.copy()
    if env:
        environ.update(env)

    kwargs = {}

    prefix = [prefix] or []
    if dry:
        prefix.append("DRY-RUN")

    prefix = ".".join(prefix)

    if isinstance(cmds, str):
        for line in cmds.rstrip().split("\n"):
            logger.info(f"{prefix}: $ {line}")
        shell = True
    elif isinstance(cmds, (list, tuple)):
        logger.info(f"{prefix} {cmds}")
        shell = False
    elif isinstance(cmds, dict):
        logger.info(f"{prefix} {cmds}")
        cmds0 = cmds.copy()
        try:
            cmds = cmds0.pop("cmd")
        except KeyError:
            raise KeyError("Dict shell commands MUST have 'cmd' defined")
        shell = cmds0.pop("shell", False)
        environ.update(cmds0.pop("env", {}))
        cmds0.pop("stdout", None)
        cmds0.pop("stderr", None)
        logger.debug(f"Cleaned cmd: {cmds0}")
        kwargs.update(cmds0)
    else:
        raise TypeError("Shell commands must be str, list/tuple, or dict")

    if dry:
        return logger.info("DRY-RUN: Not running")

    # Apply formatting. Uses the C-Style so that it is less likely to
    # have to need escaping
    if isinstance(cmds, (list, tuple)):
        cmds0 = cmds.copy()
        cmds = [cmd % environ for cmd in cmds]
        if cmds != cmds0:
            logger.debug(f"Formatted cmds: {cmds}")

    proc = subprocess.Popen(
        cmds,
        shell=shell,
        env=environ,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
        **kwargs,
    )

    out, err = proc.communicate()
    out, err = out.decode(), err.decode()
    out = out.rstrip("\n")
    err = err.rstrip("\n")

    logger.info(f"out: {out}")

    if err.strip():
        logger.info(f"err: {err}")

    if proc.returncode > 0:
        logger.info(
            f"{prefix} WARNING: Command return non-zero returncode: {proc.returncode}"
        )
    return proc.returncode


def time_format(dt, upper=False):
    """Format time into days (D), hours (H), minutes (M), and seconds (S)"""
    labels = [  # Label, # of sec
        ("D", 60 * 60 * 24),
        ("H", 60 * 60),
        ("M", 60),
        ("S", 1),
    ]
    res = []
    for label, sec in labels:
        val, dt = divmod(dt, sec)
        if not val and not res and label != "S":  # Do not skip if already done
            continue
        if label == "S" and dt > 0:  # Need to handle leftover
            res.append(f"{val+dt:0.2f}")
        elif label in "HMS":  # these get zero padded
            res.append(f"{int(val):02d}")
        else:  # Do not zero pad dats
            res.append(f"{int(val):d}")
        res.append(label if upper else label.lower())
    return "".join(res)


def randstr(N=15):
    c = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return "".join(random.choice(c) for _ in range(N))


def listify(flags):
    """Turn argument into a list. None or False-like become empty list"""
    if isinstance(flags, list):
        return flags
    flags = flags or []
    if isinstance(flags, str):
        flags = [flags]
    return list(flags)


flagify = listify


def dictify(mydict):
    """Turn argument into a dict. None or False-like become empty dict"""
    if isinstance(mydict, dict):
        return mydict
    if not mydict:
        return {}
    if isinstance(mydict, (list, tuple)):
        return dict(mydict)
    return mydict


def shell_header(config, cd=True):
    from .rclonerc import RC

    out = []
    if cd:
        cmd = ["cd", os.path.abspath(os.getcwd())]
        out.append(shlex.join(cmd))

    for key, value in config.rclone_env.items():
        if value in ("**UNSET**", RC.DELENV):
            out.append(f"unset {key}")
            continue
        out.append(f"export {key}={shlex.quote(value)}")

    return "\n".join(out)


def smart_open(filename, mode="rb"):
    filename = str(filename)
    if filename.endswith(".gz"):
        import gzip as gz

        return gz.open(filename, mode)
    elif filename.endswith(".xz"):
        import lzma as xz

        return xz.open(filename, mode)
    else:
        return open(filename, mode)


def head_tail_table(table, /, head=None, tail=None, *, header=True, dots=False):
    """
    head or tail a table.

    Inputs:
    ------
    table
        List of lists represeting a list of rows

    head [None]
        Include the first 'head' rows. If None will not include
        the head of the table unless tail is also None

    tail [None]
        Include the last 'tail' rows. If None, will not include
        the tail of the table unless head is also None

    header [True]
        Exclude the header from 'head' and always return it. If
        False, head will count the first row

    dots [False]
        If true, adds a row with '...' for all cols or a single '...'
        if the rows are not lists. Number of cols is based on the
        first row being a list or tuple and then its length
    """
    if isinstance(table[0], (list, tuple)):
        ncol = len(table[0])
        dotrow = [*["..."] * ncol]
    else:
        ncol = 0
        dotrow = "..."

    head, tail = max([0, head or 0]), max([0, tail or 0])

    if head == tail == 0:
        return table  # Nothing. Even if header

    if not (head and tail):  # no dots if not both
        dots = False

    out = []
    if header:
        out.append(table[0])
        table = table[1:]

    ixhead = set(range(head))
    ixtail = set(range(len(table) - tail, len(table)))
    ixtot = ixhead.union(ixtail)

    headfin = False
    for i, row in enumerate(table):
        # If the full table is covered, this block will never be
        # skipped. But once it is, add the dots the first time then
        # continue. This way you only walk the table once
        if i in ixtot:
            out.append(row)
            continue

        if dots and not headfin and not ixhead.intersection(ixtail):
            out.append(dotrow)
        headfin = True

    return out


def smart_splitext(file):
    """
    Split into stem,ext but allow for multiple valid extensions
    such as .tar.gz.

    Always splits the first extension but keeps going while
    the others are valid MIME types. Never includes the first
    part, even if leading dot
    """
    if not mimetypes.inited:
        mimetypes.init()

    parent, name = os.path.split(file)

    parts = name.split(".")
    if not parts[0]:  # leading dot
        parts[1] = f".{parts[1]}"
        parts = parts[1:]

    if len(parts) == 1:  # Just file.ext
        return file, ""

    # Decide where to stop. This is bounded such that it will
    # always include the first and never include the last
    for ix in range(1, len(parts)):
        if "." + parts[-ix - 1].lower() not in mimetypes.types_map:
            break

    stem = ".".join(parts[:-ix])
    ext = "." + ".".join(parts[-ix:])  # No ext covered above
    return os.path.join(parent, stem), ext


def rpath2apath(rpath, *, preferred="datetag"):
    """
    Convert the rpath to the apath with the time and flag.

    This is designed to handle a few special cases. Notably, if the flag is
    manually appended on to the file such as if done by hand (incorrectly).

    Also note that in the case of a file like "file.<date1>.<date2>" with
    no extension, it should return "file.<date2>" tagged at <date1> in accordance
    with the split.
    """
    parent, rname = os.path.split(rpath)

    # Case 1: smartsplit off ext. The tag will not be a MIME type
    #         so this will work with file.20220625232247.tar.gz
    #         and file.tar.20220625232247.gz
    # NOTE: This comes FIRST in case of "file.<date1>.<date2>"
    base_w_tag, ext = smart_splitext(rname)
    base, tag = os.path.splitext(base_w_tag)
    try:
        ts, flag, _ = parse_timestamp_tag(tag, preferred=preferred)
        apath = os.path.join(parent, f"{base}{ext}")
        return apath, ts, flag
    except ValueError:
        pass

    # Case 2: The extension is the end.
    aname, tag = os.path.splitext(rname)
    try:
        ts, flag, _ = parse_timestamp_tag(tag, preferred=preferred)
        apath = os.path.join(parent, aname)
        return apath, ts, flag
    except ValueError:
        pass

    raise NoTimestampInNameError(f"No timestamp in {rpath = }")


re_datetag = re.compile(
    r"""
    ^                        # Start of the string
    (\d{4})                  # Match any four digits representing the year (YYYY)
    (0[1-9]|1[0-2])          # Match two digits for the month (01-09, 10-12)
    (0[1-9]|[12][0-9]|3[01]) # Match two digits for the day (01-09, 10-29, 30-31)
    ([01][0-9]|2[0-3])       # Match two digits for the hour (00-23)
    ([0-5][0-9])             # Match two digits for the minutes (00-59)
    ([0-5][0-9])             # Match two digits for the seconds (00-59)
    (R|D|r|d)?               # Optionally match an "R" or "D" at the end
    $                        # End of the string
    """,
    re.VERBOSE,
)


def validate_timestamp_format(fmt):
    if fmt not in TIMESTAMP_FORMATS:
        raise ValueError(f"Unsupported timestamp format {fmt!r}")
    return fmt


def alternate_timestamp_format(fmt):
    validate_timestamp_format(fmt)
    return "base36-epoch" if fmt == "datetag" else "datetag"


def encode_base36_epoch(ts):
    ts = int(ts)
    if ts < 0:
        raise ValueError("base36-epoch requires a non-negative epoch second")
    if ts > BASE36_EPOCH_MAX:
        raise ValueError(f"base36-epoch exceeds {BASE36_EPOCH_WIDTH} characters")

    if ts == 0:
        return "0" * BASE36_EPOCH_WIDTH

    digits = []
    while ts:
        ts, rem = divmod(ts, 36)
        digits.append(BASE36_ALPHABET[rem])

    token = "".join(reversed(digits))
    return token.rjust(BASE36_EPOCH_WIDTH, "0")


def decode_base36_epoch(token):
    token = token.lower()
    if len(token) != BASE36_EPOCH_WIDTH:
        raise ValueError()
    if any(ch not in BASE36_ALPHABET for ch in token):
        raise ValueError()

    return int(token, 36)


def format_timestamp_tag(ts, *, fmt="datetag"):
    fmt = validate_timestamp_format(fmt)
    ts = int(ts)
    if fmt == "datetag":
        return time2all(ts).dt
    if ts < 0:
        raise ValueError("base36-epoch requires a non-negative epoch second")
    return encode_base36_epoch(ts)


def parse_datetag_flag(ts):
    ts = ts.removeprefix(".")
    if not (match := re_datetag.match(ts)):
        raise ValueError()
    ts, _, _, _ = time2all("".join(match.groups()[:-1]))
    flag = match.groups("")[-1].upper()
    return ts, flag


def parse_base36_epoch_flag(tag):
    tag = tag.removeprefix(".")
    token, flag = tag[:BASE36_EPOCH_WIDTH], tag[BASE36_EPOCH_WIDTH:]
    if flag not in {"", "D", "R", "d", "r"}:
        raise ValueError()
    ts = decode_base36_epoch(token)
    return ts, flag.upper()


def parse_timestamp_tag(tag, *, preferred="datetag"):
    preferred = validate_timestamp_format(preferred)
    for fmt in (preferred, alternate_timestamp_format(preferred)):
        try:
            if fmt == "datetag":
                ts, flag = parse_datetag_flag(tag)
            else:
                ts, flag = parse_base36_epoch_flag(tag)
            return ts, flag, fmt
        except ValueError:
            pass
    raise ValueError()


def parse_dateflag(ts):
    ts, flag, _ = parse_timestamp_tag(ts)
    return ts, flag


def timestamp_to_epoch(ts, *, preferred="datetag"):
    if isinstance(ts, str):
        if re_datetag.match(ts):
            return parse_datetag_flag(ts)[0]
        tag = ts.removeprefix(".")
        if len(tag) == BASE36_EPOCH_WIDTH and all(
            ch.lower() in BASE36_ALPHABET for ch in tag
        ):
            return decode_base36_epoch(tag)
    return time2all(ts).ts


def apath2rpath(apath, ts=None, *, flag="", verify=True, fmt="datetag"):
    """
    Convert from apath,ts ('sub/dir/file.txt',12345)
    to rpath ('sub/dir/file.12345.txt')

    Will not be correct for references but *will* give the
    referrer path
    """
    from . import nowfun  # Avoid circular import

    ts = ts or nowfun()[0]
    ts = timestamp_to_epoch(ts, preferred=fmt)
    dt = format_timestamp_tag(ts, fmt=fmt)

    base, ext = smart_splitext(apath)
    rpath = f"{base}.{dt}{flag}{ext}"

    # Comment this out b/c older split names will give a false positive.
    # if _verify and rpath2apath(rpath,_verify=False)[0] != apath:
    #     logger.error(
    #         f"Failed round-trip sanity check with {apath = }, {rpath = }. "
    #         "Please submit a bug report"
    #     )

    # Sanity check:
    sanity_ok = True
    if verify:
        try:
            sanity_ok = rpath2apath(rpath, preferred=fmt) == (apath, ts, flag)
        except (NoTimestampInNameError, ValueError):
            sanity_ok = False

    if verify and not sanity_ok:
        logger.warning(
            f"Failed sanity check {apath = }, {rpath = }. Using fallback split"
        )
        base, ext = os.path.splitext(apath)
        rpath = f"{base}.{dt}{flag}{ext}"
    return rpath
