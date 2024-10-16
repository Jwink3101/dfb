import os, sys
import io
import shutil
from pathlib import Path
import datetime, time
import random
import subprocess
import hashlib
import shutil
import atexit
import logging
from functools import cached_property

logger = logging.getLogger(__name__)

PWD0 = os.path.abspath(os.path.dirname(__file__))
os.chdir(PWD0)

p = os.path.abspath("../")
if p not in sys.path:
    sys.path.insert(0, p)

import dfb.configuration
import dfb.cli
import dfb.dstdb
from dfb.rclonecli import RcloneCLI as RcloneAPI
from dfb.rclonerc import random_port

_r = repr

dfb.cli._TESTMODE = True


class StringBufferBytesIO(io.TextIOWrapper):
    """
    Acts like io.StringIO but also supports .buffer for bytes
    """

    def __init__(self, buffer=None, encoding="utf-8"):
        # If no buffer is provided, create a new BytesIO buffer
        if buffer is None:
            buffer = io.BytesIO()
        super().__init__(buffer, encoding=encoding)

    @property
    def bytes_buffer(self):
        return self.buffer.raw


class Capture:
    def __enter__(self):
        self.old_stdout = sys.stdout
        self.old_stderr = sys.stderr

        self.stdout_buffer = io.BytesIO()
        self.stderr_buffer = io.BytesIO()

        sys.stdout = io.TextIOWrapper(self.stdout_buffer, encoding="utf-8")
        sys.stderr = io.TextIOWrapper(self.stderr_buffer, encoding="utf-8")

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        sys.stdout.flush()
        sys.stderr.flush()
        self.stdout_buffer.flush()
        self.stderr_buffer.flush()

        self.out_bytes = self.stdout_buffer.getvalue()
        self.err_bytes = self.stderr_buffer.getvalue()
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr

    @cached_property
    def out(self):
        return self.out_bytes.decode("utf-8")

    @cached_property
    def err(self):
        return self.err_bytes.decode("utf-8")


class Tester:
    def __init__(self, *, name, src=None, dst=None, seed=1):
        os.chdir(PWD0)

        random.seed(seed)

        dfb.configuration._TEMPDIR = "TEMP"
        dfb._override_ts = "1970-01-01 00:00:00Z"  # Offsets = unix time

        self.pwd = Path(os.path.abspath(f"testdirs/{name}"))
        self.make_ignore()

        if src is None:
            src = os.path.join(self.pwd, "src")
        if dst is None:
            dst = os.path.join(self.pwd, "dst")

        self.src = src
        self.dst = dst

        self.config = {
            "src": self.src,
            "dst": self.dst,
            "rclone_env": {
                "RCLONE_CACHE_DIR": str((self.pwd / "cache").resolve()),
                "RCLONE_PASSWORD_COMMAND": RcloneAPI.DELENV,
                "RCLONE_CONFIG": str(self.pwd / "rclone.cfg"),
            },
            "config_id": f"test_{name}",
        }
        self.configfile = str(self.pwd / "config.py")

        try:
            shutil.rmtree(self.pwd)
        except OSError:
            pass

        os.makedirs(self.pwd)
        shutil.copy2("rclone.cfg", self.pwd / "rclone.cfg")

        os.chdir(self.pwd)

        self.logs = []

        self.backup_local_files = {}

        # shutil.copy2("rclone.cfg", self.pwd / "rclone.cfg")
        # self.config["rclone_env"] = {"RCLONE_CONFIG": "rclone.cfg"}

    def write_config(self):
        with open(self.configfile, "wt") as fobj:
            for key, val in self.config.items():
                print(f"{key} = {val!r}", file=fobj)

        self.config_obj = dfb.configuration.Config(self.configfile).parse()
        self.dstdb = dfb.dstdb.DFBDST(self.config_obj)
        return self.config_obj

    def call(self, cmd0, *args, offset=None):
        """Call with a specified config and offset"""
        if offset is None:
            offset = 2 * len(self.logs) + 1
        dfb._override_offset = offset

        r = dfb.cli.cli([cmd0, *args, "--config", self.configfile])

        logfile = self.config_obj.logfile.resolve()
        debugfile = self.config_obj.debuglogfile.resolve()

        tt = time.time_ns() // 1000

        if logfile.exists():
            logtext = logfile.read_text()
            dest = f"{logfile.stem}.{tt}{logfile.suffix}"
            logfile.rename(dest)
        else:
            logtext = ""

        if debugfile.exists():
            debugtext = debugfile.read_text()
            dest = f"{debugfile.stem}.{tt}{debugfile.suffix}"
            debugfile.rename(dest)
        else:
            debugtext = ""

        self.logs.append((logtext, debugtext))
        return r

    def ls(self, *cmd, **kw):
        with Capture() as cap:
            self.call("ls", *cmd, **kw)
        out = cap.out
        print(out)
        return out

    def tree(self, *cmd, **kw):
        with Capture() as cap:
            self.call("tree", *cmd, **kw)
        out = cap.out
        print(out)
        return out

    def backup(self, *args, offset=None, allow_error=False):
        backobj = self.call("backup", *args, offset=offset)
        if not backobj and allow_error:
            return

        self.backup_local_files[offset] = self.local_files()

        return backobj

    def make_ignore(self, file=".ignore"):
        ignore = self.pwd.parent / file
        if not ignore.exists():
            self.pwd.parent.mkdir(exist_ok=True, parents=True)
            with ignore.open(mode="a"):
                pass

    def write(self, path, content, mode="wt", dt=0):
        try:
            os.makedirs(os.path.dirname(path))
        except:
            pass

        with open(path, mode) as file:
            file.write(content)

        if dt:
            change_time(path, dt)

        # Make the times integers to avoid issues
        stat = os.stat(path)
        os.utime(path, (stat.st_atime, stat.st_mtime))

    def write_pre(self, path, content, mode="wt", dt=None):
        """Write items randomly in the past"""
        dt = dt if not None else -5 * (1 + random.random())
        if path.startswith("B"):
            raise ValueError("No pre on B")
        self.write(path, content, mode=mode, dt=dt)

    def write_post(self, path, content, mode="wt", add_dt=0):
        """
        Write items randomly in the future. Can add even more if forcing
        newer
        """
        dt = 5 * (1 + random.random()) + add_dt
        self.write(path, content, mode=mode, dt=dt)

    def read(self, path):
        with open(path, "rt") as file:
            return file.read().strip()

    @staticmethod
    def sha1(path):
        hh = hashlib.sha1()
        with open(path, "rb") as file:
            while dat := file.read(1024 * 512):
                hh.update(dat)
        return hh.hexdigest()

    def globread(self, globpath):
        paths = glob.glob(globpath)
        if len(paths) == 0:
            raise OSError("No files matched the glob pattern")
        if len(paths) > 1:
            raise OSError(f"Too many files matched the pattern: {paths}")

        return self.read(paths[0])

    def move(self, src, dst):
        try:
            os.makedirs(os.path.dirname(dst))
        except OSError:
            pass

        shutil.move(src, dst)

    def remote_snapshot(self, **kwargs):
        """
        Use the snapshot utils to list at a certain time
        """
        files = set()
        for file in map(self.dstdb.fullrow2dict, self.dstdb.snapshot(**kwargs)):
            row = {
                "apath": file["apath"],
                "size": file["size"],
                "mtime": int(file["mtime"]),
                "sha1": self.sha1(os.path.join(self.config_obj.dst, file["rpath"])),
            }
            row = frozenset(row.items())
            files.add(row)
        return files

    def local_files(self, path=None):
        files = set()
        path = path or self.src
        for file in tree(path):
            stat = os.stat(file)
            row = {
                "apath": os.path.relpath(file, path),
                "size": stat.st_size,
                "mtime": int(stat.st_mtime),
                "sha1": Tester.sha1(file),
            }
            row = frozenset(row.items())
            files.add(row)
        return files

    def tree_sha1s(self, path):
        files = set()
        for file in tree(path):
            files.add(Tester.sha1(file))
        return files

    def all_src_in_dst(self, return_diff=False):
        """return if all source files exists *somewhere* in the dest"""
        diff = self.tree_sha1s(self.src) - self.tree_sha1s(self.dst) == set()
        if return_diff:
            return diff
        return diff == set()

    def src_missing_in_dst(self, keys=None, **kwargs):
        diff = self.local_files() - self.remote_snapshot(**kwargs)
        if not keys:
            return diff
        if isinstance(keys, str):
            keys = [keys]
        return {frozenset(i for i in d if i[0] in keys) for d in diff}

    @property
    def src_rclone(self):
        return self.config_obj.src_rclone

    @property
    def dst_rclone(self):
        return self.config_obj.dst_rclone


def change_time(path, time_adj):
    """Change the time on a file path"""
    stat = os.stat(path)
    os.utime(path, (stat.st_atime + time_adj, stat.st_mtime + time_adj))


def tree(path, hidden=False):
    files = []
    for dirpath, dirnames, filenames in os.walk(path, followlinks=True):
        exc = {".DS_Store"}
        filenames = [f for f in filenames if f not in exc]
        if not hidden:
            filenames[:] = [f for f in filenames if not f.startswith(".")]
            dirnames[:] = [d for d in dirnames if not d.startswith(".")]

        filenames.sort(key=str.lower)
        dirnames.sort(key=str.lower)

        files.extend(os.path.join(dirpath, filename) for filename in filenames)

    return files


def dict2frozen(item):
    if isinstance(item, dict):
        return frozenset((k, dict2frozen(v)) for k, v in item.items())
    if isinstance(item, list):
        return tuple(dict2frozen(i) for i in item)
    if isinstance(item, set):
        return frozenset(item)
    return item


def venn(A, B):
    A = set(A)
    B = set(B)
    return {r"A\B": A - B, "A∩B": A.intersection(B), r"B\A": B - A}


class WebDAV:
    def __init__(self, path=None, ip="localhost", port=None):
        self.port = port or random_port()
        self.ip = ip
        self.pwd = path or os.getcwd()
        self.running = False

    @property
    def remote(self):
        if not self.running:
            return
        return f":webdav,url='http://{self.ip}:{self.port}',vendor=rclone:"

    def start(self):
        atexit.register(self.stop)

        cmd = [
            "rclone",
            "serve",
            "webdav",
            self.pwd,
            "--addr",
            f"{self.ip}:{self.port}",
        ]
        logger.debug(f"WEBDAV: {cmd = }")
        self.proc = subprocess.Popen(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        self.running = True
        logger.debug("START WEBDAV")
        return self

    def stop(self):
        if not self.running:
            return
        import signal

        self.proc.send_signal(signal.SIGKILL)
        logger.debug("END WEBDAV")
        self.running = False

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *_, **__):
        self.stop()
