"""
Utilities
"""
import os
import datetime
from collections import namedtuple
import sqlite3
import random
import subprocess
import shlex

# This can be standalone but will just use the one in rcloneapi which is designed to
# be its own
from .timestamps import timestamp_parser
from . import log, debug

tsrep = namedtuple("timestamps", ("ts", "dt", "obj", "pretty"))

_r = repr


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


## Test to include later
# ts = int(time.time())
# tss = [time2all(ts)]
# r = ts
# for i in range(25):
#     r0 = r
#     alltimes = time2all(r)
#     r = alltimes[ i % 2 ]
#     tss.append(alltimes)
# assert all(t == tss[0] for t in tss)


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


def bytes2human(byte_count, base=1024, short=True):
    """
    Return a value,label tuple
    """
    if base not in (1024, 1000):
        raise ValueError("base must be 1000 or 1024")

    labels = ["kilo", "mega", "giga", "tera", "peta", "exa", "zetta", "yotta"]
    name = "bytes"
    if short:
        labels = [f"{l[0].upper()}i" for l in labels]
        name = name[0].upper()
        # see https://www.ibm.com/docs/en/spectrum-control/5.4.2?topic=concepts-units-measurement-storage-data
    labels.insert(0, "")

    best = 0
    for ii in range(len(labels)):
        if (byte_count / (base**ii * 1.0)) < 1:
            break
        best = ii

    return byte_count / (base**best * 1.0), labels[best] + name


def shell_runner(cmds, dry=False, env=None, prefix=""):
    """
    Run the shell command (string or list) and return the returncode
    """

    environ = os.environ.copy()
    if env:
        environ.update(env)

    kwargs = {}

    prefix = [prefix] if prefix else []
    if dry:
        prefix.append("DRY-RUN")

    if isinstance(cmds, str):
        for line in cmds.rstrip().split("\n"):
            log(f"$ {line}", prefix=prefix)
        shell = True
    elif isinstance(cmds, (list, tuple)):
        log(f"{cmds}", prefix=prefix)
        shell = False
    elif isinstance(cmds, dict):
        log(f"{cmds}", prefix=prefix)
        cmds0 = cmds.copy()
        try:
            cmds = cmds0.pop("cmd")
        except KeyError:
            raise KeyError("Dict shell commands MUST have 'cmd' defined")
        shell = cmds0.pop("shell", False)
        environ.update(cmds0.pop("env", {}))
        cmds0.pop("stdout", None)
        cmds0.pop("stderr", None)
        debug(f"Cleaned cmd: {cmds0}")
        kwargs.update(cmds0)
    else:
        raise TypeError("Shell commands must be str, list/tuple, or dict")

    if dry:
        return log("DRY-RUN: Not running")

    # Apply formatting. Uses the C-Style so that it is less likely to
    # have to need escaping
    if isinstance(cmds, (list, tuple)):
        cmds0 = cmds.copy()
        cmds = [cmd % environ for cmd in cmds]
        if cmds != cmds0:
            debug(f"Formatted cmds: {cmds}")

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

    log(f"{out}", prefix=prefix + ["out"])

    if err.strip():
        log(f"{err}", prefix=prefix + ["err"])

    if proc.returncode > 0:
        log(
            f"WARNING: Command return non-zero returncode: {proc.returncode}",
            prefix=prefix,
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


# def removeprefix(mystr, prefix):
#     if mystr.startswith(mystr):
#         return mystr[len(mystr) :]
#     return mystr


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
    from .rclone import RC

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
