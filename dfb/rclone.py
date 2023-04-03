import urllib.parse
import json
import time
import signal
import os, sys
import re
import math
import tempfile
import socket
import subprocess
from collections import defaultdict
from functools import partialmethod, cache
from threading import Thread
from queue import Queue
import atexit


from . import log, debug
from .rcloneapi import RcloneFile
from .utils import randstr, dictify, listify
from .timestamps import timestamp_parser

import requests
from requests.auth import HTTPBasicAuth


class RcloneError(ValueError):
    pass


class RC:
    """
    Rclone rc command controller.

    Still a bit of a work in progress, especially around documentation.

    Note that all filepaths (e.g. src, dst) will use rcpathsplit which accepts
    a regular path and heuristically breaks it into filesystem (fs) and remote/file or
    specify a 2-tuple of (fs,remote/file)

    rclone_exe ['rclone']
        Executable to call

    serve_flags [None]
        Flags to always include. Could be thinks like config specification

    rclone_env (None / Empty dict)
        Environment to awlays include. This is appended to os.environ.
        If there is something in os.environ that is *not* wanted, specify it
        with RLCONE.DELENV. ex:
            {"RCLONE_PASSWORD_COMMAND": RC.DELENV}

    serve_log_callback
        Function with the signature serve_log_callback(out_or_err,line)
        where out_or_err is "stdout" or "stderr" and the line is the result

    """

    DELENV = "**DELENV**"  # Remove from environment
    NOFLAG = "**NOFLAG**"  # Remove from call (Not sure this is used)

    def __init__(
        self,
        rclone_exe="rclone",
        serve_flags=None,
        rclone_env=None,
        serve_log_callback=None,
    ):
        self.rclone_exe = rclone_exe
        self.addr = f"localhost:{random_port()}"
        self.serve_flags = listify(serve_flags)
        self.rclone_env = dictify(rclone_env)
        self.serve_log_callback = serve_log_callback
        self.user = randstr()
        self.password = randstr()
        debug(f"http://{self.user}:{self.password}@{self.addr}")

        self._started = False
        self._exit = False

    def __enter__(self):
        self.start_rc()
        return self

    def __exit__(self, *_):
        self.stop_rc()

    def start_rc(self):
        if self._started:
            debug("Already Started")
            return

        cmd = [self.rclone_exe, "rcd"] + self.serve_flags
        cmd.append("--rc-serve")  # For reading remote content
        cmd.extend(["--rc-addr", self.addr])
        cmd.extend(["--rc-user", self.user])
        cmd.extend(["--rc-pass", self.password])
        cmd.extend(["--rc-server-read-timeout", "100h"])
        cmd.extend(["--rc-server-write-timeout", "100h"])
        cmd.extend(["--log-format", ""])

        env = os.environ.copy()
        env.update({str(k): str(v) for k, v in self.rclone_env.items()})
        for key in list(env):
            if env[key] == self.DELENV:
                del env[key]
        e = {k: v for k, v in env.items() if k not in os.environ}
        debug(f"rclone call {str(cmd)} with env: {json.dumps(e)}")

        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )

        self._started = True

        self.server_reader_thread = Thread(target=self.server_reader, daemon=True)
        self.server_reader_thread.start()

        self._wait_for_start()

    def server_reader(self):
        if self.serve_log_callback:
            cb = self.serve_log_callback
        else:
            cb = lambda *_, **__: None

        for oe, line in popen_streamer(self.proc, allow_error=True):
            if self.serve_log_callback:
                cb(oe, line.decode().rstrip("\n"))

            if self._exit:
                break

    def _wait_for_start(self, dt=0.2, timeout=5):
        n = math.ceil(timeout / dt)
        for i in range(n):
            try:
                self.call("rc/noop")
                debug(f"Started at {i = }")
                break
            except requests.ConnectionError:  # ConnectionError
                pass
            time.sleep(dt)
        else:
            raise ValueError("Failed to start server")

    def stop_rc(self):
        if not self._started:
            return
        self._exit = True
        self.call("core/quit")
        self.proc.send_signal(signal.SIGINT)
        try:
            self.proc.wait(timeout=1)
        except:
            self._kill()

    def _kill(self):
        try:
            self.proc.send_signal(signal.SIGKILL)
        except:
            pass

    def kill_at_exit(self):
        atexit.register(self._kill)

    def _cpmvfile(self, *, cpmv, src, dst, use_async=False, **params):
        """
        src and dst can either be strings or Fs,Remote tuples. The latter will
        optimize the calls since it will cache more information
        """
        endpoint = f"operations/{cpmv}"
        method = self.call_async if use_async else self.call

        srcFs, srcRemote = rcpathsplit(src)
        dstFs, dstRemote = rcpathsplit(dst)
        params["srcFs"] = srcFs
        params["srcRemote"] = srcRemote
        params["dstFs"] = dstFs
        params["dstRemote"] = dstRemote
        return method(endpoint, params=params)

    copyfile = partialmethod(_cpmvfile, cpmv="copyfile")
    movefile = partialmethod(_cpmvfile, cpmv="movefile")

    def delete(self, file, use_async=False, **params):
        method = self.call_async if use_async else self.call

        endpoint = f"operations/deletefile"
        params["fs"], params["remote"] = rcpathsplit(file)

        return method(endpoint, params=params)

    def write(self, dst, content, use_async=False, **params):
        endpoint = "operations/uploadfile"
        method = self.call_async if use_async else self.call

        params["fs"], name = rcpathsplit(dst)
        params["remote"], name = os.path.split(name)

        if isinstance(content, str):
            content = content.encode()
        try:
            return self.call(
                endpoint,
                params=params,
                postkw=dict(files={name: content}),
            )
        except:
            log("Trying write fallback")
            params.pop("fs", None)
            params.pop("remote", None)
            return self._write_fallback(dst, content, use_async=use_async, **params)

    def _write_fallback(self, dst, content, use_async=False, **params):
        _config = params.get("_config", {})
        _config["NoCheckDest"] = True
        params["_config"] = _config

        if isinstance(content, str):
            content = content.encode()

        with tempfile.NamedTemporaryFile() as fp:
            fp.write(content)
            fp.flush()

            return self.copyfile(
                src=fp.name,
                dst=dst,
                use_async=use_async,
                **params,
            )

    def read(self, src):
        self.start_rc()

        fs, file = rcpathsplit(src)
        file_url = urllib.parse.urljoin(
            f"http://{self.user}:{self.password}@{self.addr}", f"[{fs}]/{file}"
        )
        try:
            res = requests.get(file_url)
            if res.status_code == 404:
                raise ValueError("Not Found")
            return res.content
        except:
            log("Trying read fallback")
            return self._read_fallback(src)

    def _read_fallback(self, src, use_async=False, **params):
        """
        There is no [currently] a cat-like command so just use a temp file.
        This isn't ideal but using an 'rclone cat' command doesn't let you keep
        the same authorization and, typically, these files should be very small
        """
        _config = params.get("_config", {})
        _config["NoCheckDest"] = True
        params["_config"] = _config

        with tempfile.NamedTemporaryFile() as fp:
            dst = fp.name
            self.copyfile(
                src=src,
                dst=fp.name,
                use_async=use_async,
                **params,
            )
            return fp.read()

    def listremote(
        self,
        *,
        fs,
        remote="",
        filters=None,
        filter_from=None,
        exclude_if_present=None,
        mimetype=False,
        modtime=True,
        hashes=False,
        hashtypes=False,
        metadata=False,
        maxdepth=None,
        only=None,
        epoch_time=False,
        filter_params=None,
        config_params=None,
        use_async=False,
        **params,
    ):
        """
        List remotes

        Inputs:
        -------
        fs
            Remote file system

        filters [empty]
            Filter or list of filters

        filter_from
            Filter from file or list of files

        exclude_if_present
            Filename or list of filenames to use to mean ignore the directory

        mimetype [False]
            If False, will call with equiv of --no-mimetype

        modtime [True]
            If False, will call with equiv of --no-modtime. This can be faster on remotes
            that require additional requests for it. Note that it still may or may not
            include a ModTime. ModTimes are converted to datetime objects unless
            epoch_time=True.

        hashes [False]
            Compute hashes

        hashtypes [None]
            If specified and hashes=True, will use these types. String of one or list
            of types

        metadata [False]
            If True, adds --metadata

        maxdepth [None]
            Adds equiv of --max-depth flags.

        only
            Specify as {None [default],'files','dirs'}.

        epoch_time [False]
            If True, returns a Unix Epoch float. Otherwise, returns a time zone aware
            datetime object

        filter_params [empty]
            Additional filter dictionary. See options/get and the 'filter' key.
            'DeleteExcluded', 'ExcludeFile', 'ExcludeFrom', 'ExcludeRule', 'FilesFrom',
            'FilesFromRaw', 'FilterFrom', 'FilterRule', 'IgnoreCase', 'IncludeFrom',
            'IncludeRule', 'MaxAge', 'MaxSize', 'MinAge', 'MinSize'

        config_params [empty]
           Additional _config dictionary. See options/get
        """
        # Build the call. First params
        params["fs"] = fs
        params["remote"] = remote

        # Filters. Allow for _filter in params, filter_params, and finally, the reg kws
        filter_params = params.get("_filter", {}) | dictify(filter_params)

        filter_params["FilterRule"] = listify(filter_params.get("FilterRule", []))
        filter_params["FilterRule"].extend(listify(filters))

        filter_params["FilterFrom"] = listify(filter_params.get("FilterFrom", []))
        filter_params["FilterFrom"].extend(listify(filter_from))

        filter_params["ExcludeFile"] = listify(filter_params.get("ExcludeFile", []))
        filter_params["ExcludeFile"].extend(listify(exclude_if_present))

        params["_filter"] = filter_params

        # Other config
        params["_config"] = params.get("_config", {}) | dictify(config_params)

        # Rest of the KWs
        opt = params.get("opt", {})
        opt["recurse"] = True
        opt["noMimeType"] = not mimetype
        opt["noModTime"] = not modtime
        opt["showHash"] = hashes
        if hashtypes and hashes:
            opt["hashTypes"] = listify(hashtypes)
        opt["metadata"] = metadata

        if only is None:
            pass
        elif only == "dirs":
            opt["dirsOnly"] = True
        elif only == "files":
            opt["filesOnly"] = True
        else:
            raise ValueError(f"Inalid {only = }")

        if maxdepth:
            params["_config"]["MaxDepth"] = maxdepth

        params["opt"] = opt

        method = self.call_async if use_async else self.call

        res = method("operations/list", params=params)
        if "list" not in res:
            raise ValueError(f"Error listing remote. Check keywords. {res = }")
        for line in res["list"]:
            # Never understood why rclone gives us this...
            line.pop("Name", None)

            if "ModTime" in line:  # Do regardless of modtime setting
                line["ModTime"] = timestamp_parser(line["ModTime"], epoch=epoch_time)
        return res["list"]

    @cache
    def features(self, fs, **params):
        params["fs"] = fs
        return self.call("operations/fsinfo", params=params)

    @cache
    def paths(self):
        r = self.call("core/command", params={"command": "config", "arg": ["paths"]})
        paths = {}
        for line in r["result"].splitlines():
            line = line.strip()
            if not line:
                continue
            key, val = line.split(":", 2)
            paths[key.strip()] = val.strip()
        return paths

    def call(self, endpoint, *, postkw=None, params=None):
        self.start_rc()

        postkw = postkw or {}
        params = params or {}

        for key, val in params.items():
            if isinstance(val, (dict, list)):
                params[key] = json.dumps(val)

        # In order to get sending data for rcat to work, we use the URL
        # paramaters
        url = (
            urllib.parse.urljoin(f"http://{self.addr}", endpoint)
            + "?"
            + urllib.parse.urlencode(params)
        )

        resp = requests.post(
            url,
            auth=HTTPBasicAuth(self.user, self.password),
            **postkw,
        )
        res = resp.json()
        debug(f"call {res = }")
        if res.get("error", ""):
            raise RcloneError(f"Error. Result: {res}")
        return res

    def call_async(self, endpoint, postkw=None, params=None):
        params = params or {}
        params = params.copy()
        params["_async"] = True
        jobid = self.call(endpoint, postkw=postkw, params=params)["jobid"]
        t0 = time.time()

        while True:
            elapsed = time.time() - t0
            dt = dtfun(elapsed)
            res = self.call("job/status", params={"jobid": jobid})
            if res.get("finished", False) or res.get("error", ""):
                break
            time.sleep(dt)
        return res


def rcpathsplit(path):
    """
    Splits the fs and the remote while acounting for special remotes and connection
    strings. If given as a 2-tuple or 2-list, will just return the value.

        rcpathsplit('single-file.ext') = ('./', 'single-file.ext')
        rcpathsplit('local/file.ext') = ('local', 'file.ext')
        rcpathsplit('remote:file.ext') = ('remote:', 'file.ext')
        rcpathsplit('remote:sub/file.ext') = ('remote:', 'sub/file.ext')
        rcpathsplit('remote:/sub/file.ext') = ('remote:', '/sub/file.ext')
        rcpathsplit(':http:sub/file.ext') = (':http:', 'sub/file.ext')
        rcpathsplit(":http,url='https://example.com':path/to/dir") = (":http,url='https://example.com':", 'path/to/dir')
        rcpathsplit(":http,url='https://example.com':path/t'o/dir/with'quote") = (":http,url='https://example.com':", "path/t'o/dir/with'quote")

    The algorithim is heuristic but should account for most cases. It removes the leading ':' if
    present (from an on-the-fly remote). Then it removes everything that is quoted. Afterwards, it splits at :
    and replaces the removed values. It does not require quotes to be matched anywhere but in the remote
    name but that is also an rclone requirement
    """
    if isinstance(path, (tuple, list)) and len(path) == 2:
        return path

    if isinstance(path, RcloneFile):
        return path.fs_remote

    path = str(path)

    otf = False
    if path.startswith(":"):  # on the fly
        otf = True
        path = path[1:]

    reQUOTE = re.compile(
        r"""
         '{3}(?:[^\\]|\\.|\n)+?'{3}        # 3 single ticks
        |\"{3}(?:[^\\]|\\.|\n)+?\"{3}      # 3 double ticks
        |\".+?\"                           # 1 double tick
        |'.+?'                             # 1 single tick
        """,
        flags=re.MULTILINE | re.DOTALL | re.UNICODE | re.VERBOSE,
    )  # Regex to capture quoted statements

    reps = defaultdict(randstr)
    unquoted = reQUOTE.sub(lambda m: reps[m.group(0)], path)

    split = unquoted.split(":", 1)
    if len(split) == 1:  # Local path
        fs, remote = os.path.split(split[0])
        if not fs:
            fs = "./"
    else:
        fs, remote = split
        fs = f"{fs}:"
        if otf:
            fs = f":{fs}"

    for a, b in reps.items():
        fs = fs.replace(b, a)
        remote = remote.replace(b, a)
    return fs, remote


def rcpathjoin(*args, local_root=False):
    """
    This is like os.path.join but does some rclone-specific things because
    there could be a ':' in the first part.

    The second argument could be '/file', or 'file' and the first could have a colon.
        Rclone.pathjoin('a','b')   # a/b
        Rclone.pathjoin('a:','b')  # a:b
        Rclone.pathjoin('a:','/b') # a:/b # Note that these are unlike os.path.join
        Rclone.pathjoin('a','/b')  # a/b

    """
    args = [str(a) for a in args]  # Pathlib

    if len(args) <= 1:
        return "".join(args)

    root, first, rest = args[0], args[1], args[2:]

    if root.endswith("/"):
        root = root[:-1]

    if root.endswith(":") or first.startswith("/"):
        path = root + first
    else:
        path = f"{root}/{first}"

    path = os.path.join(path, *rest)
    return path


def random_port():
    with socket.socket() as sock:
        sock.bind(("", 0))
        return sock.getsockname()[1]


def dtfun(elapsed, dtmin=0.2, dtmax=1.5, k=0.15):
    """
    Standard logistic curve that is shifted into [dtmin,dtmax]
    with factor k
    """
    e = 2.718281828459045
    L = 1 / (1 + e ** (-k * elapsed))  # [0.5,1]
    L = (L - 0.5) / 0.5  # [0,1]
    L = (dtmax - dtmin) * L + dtmin  # [dtmin,dtmax]
    return L


def popen_streamer(proc, allow_error=False):
    """
    Takes a subprocess.Popen object and yields stdout and stderr
    simultaneously

    The proc must be opened with
        stdout=subprocess.PIPE,stderr=subprocess.PIPE

    Note that if the underlying process doesn't flush the output,
    it may not happen as expected.

    if PIPE is False, tells this function that subprocess if being
    written to files and will read them with a poll time of 1/100th
    of a second.

    Yields:
        ('stdout' or 'stderr', line)
    """

    Q = Queue()

    def _reader(oe):
        file = getattr(proc, oe)
        with file:
            for line in file:
                Q.put((oe, line))

        Q.put((oe, None))  # Done

    outthread = Thread(target=_reader, args=("stdout",))
    errthread = Thread(target=_reader, args=("stderr",))
    outthread.start(), errthread.start()

    c = 0
    while c < 2:
        oe, line = Q.get()
        if line is None:
            c += 1
            Q.task_done()
            continue
        yield oe, line
        Q.task_done()

    proc.wait()  # Should be done executing already
    if not allow_error:
        check_returncode(proc)

    Q.join()
    outthread.join()
    errthread.join()


FILTER_FLAGS = frozenset(
    {
        "--include",
        "--exclude",
        "--include-from",
        "--exclude-from",
        "--filter",
        "--filter-from",
        "--files-from",
        "--one-file-system",
        "--exclude-if-present",
    }
)

IGNORED_FILE_DATA = frozenset(
    [
        "IsDir",
        "Name",
        "ID",
        "Tier",
    ]
)
