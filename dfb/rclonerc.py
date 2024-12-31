import urllib.parse
import json
import io
import time
import signal
import os, sys
import re
import math
import tempfile
import socket
import subprocess
import argparse
import shlex
import atexit
import logging
from collections import defaultdict
from functools import partialmethod, cache
from threading import Thread
from queue import Queue

from .utils import randstr, dictify, listify
from .timestamps import timestamp_parser
from .cli import ThrowingArgumentParserError, ThrowingArgumentParser

import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)
serve_logger = logging.getLogger(f"{__name__}-rc-server")


class RcloneError(ValueError):
    pass


class RC:
    """
    rclone RC interface.

    Still a bit of a work in progress, especially around documentation.

    Note that all filepaths (e.g. src, dst) will use rcpathsplit which accepts
    a regular path and heuristically breaks it into filesystem (fs) and remote/file or
    specify a 2-tuple of (fs,remote/file). If fs is known as separate, this will be
    optimal as rclone caches information by fs.

    rclone_exe ['rclone']
        Executable to call

    serve_flags [None]
        Flags to always include. Examples includes --config, -vv, etc

        (note if using -vv, set serve_log_callback to something)

    rclone_env (None / Empty dict)
        Environment to awlays include. This is appended to os.environ.
        If there is something in os.environ that is *not* wanted, specify it
        with RLCONE.DELENV. ex:
            {"RCLONE_PASSWORD_COMMAND": RC.DELENV}

    Note:
    -----
    The server outputs to a different logger than the rest so it can be filtered.
    This doesn't make much difference if not started with -vv

    """

    DELENV = "**DELENV**"  # Remove from environment
    NOFLAG = "**NOFLAG**"  # Remove from call (Not sure this is used)

    def __init__(
        self,
        rclone_exe="rclone",
        serve_flags=None,
        rclone_env=None,
        # serve_log_callback=None,
    ):
        self.rclone_exe = rclone_exe
        self.addr = f"localhost:{random_port()}"
        self.serve_flags = listify(serve_flags)
        self.rclone_env = dictify(rclone_env)
        # self.serve_log_callback = serve_log_callback

        self.user = randstr()
        self.password = randstr()

        self._started = False
        self._exit = False

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *_):
        self.stop()

    def check(self):
        try:
            self.call("rc/noop")
            return True
        except requests.ConnectionError:
            return False

    def start(self, check=False):
        if check:
            self._started = self.check()

        if self._started:
            return self

        logger.debug("Starting rclone rc server")
        logger.debug(f"http://{self.user}:{self.password}@{self.addr}")

        cmd = [self.rclone_exe, "rcd"] + self.serve_flags
        cmd.append("--rc-serve")  # For reading remote content
        cmd.extend(["--rc-addr", self.addr])
        cmd.extend(["--rc-user", self.user])
        cmd.extend(["--rc-pass", self.password])
        cmd.extend(["--rc-server-read-timeout", "100h"])
        cmd.extend(["--rc-server-write-timeout", "100h"])
        cmd.extend(["--log-format", ""])  # dates can be captured with internal logging

        env = os.environ.copy()
        env.update({str(k): str(v) for k, v in self.rclone_env.items()})
        for key in list(env):
            if env[key] == self.DELENV:
                del env[key]
        e = {k: v for k, v in env.items() if k not in os.environ}
        logger.debug(f"rclone call {str(cmd)} with env: {json.dumps(e)}")

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
        atexit.register(self.stop)
        return self

    def server_reader(self):
        for oe, line in popen_streamer(self.proc, allow_error=True):
            line = line.decode().rstrip("\n")
            for ll in line.split("\n"):
                # serve_logger.debug(f"{oe}: {ll}")
                serve_logger.debug(ll)

            if self._exit:
                break

    def _wait_for_start(self, dt=0.2, timeout=5):
        n = math.ceil(timeout / dt)
        for i in range(n):
            try:
                self.call("rc/noop")
                logger.debug(f"Started at {i = }")
                break
            except requests.ConnectionError:  # ConnectionError
                pass
            time.sleep(dt)
        else:
            raise ValueError("Failed to start server")

    def stop(self):
        logger.debug("stopping rclone rc server")
        if not self._started:
            return
        self._exit = True

        try:
            self.call("core/quit")
        except:
            pass

        self.proc.send_signal(signal.SIGINT)
        try:
            self.proc.wait(timeout=0.25)
        except:
            try:
                self.proc.send_signal(signal.SIGKILL)
            except:
                pass
        return self

    def _cpmvfile(self, *, cpmv, src, dst, use_async=False, **params):
        """
        src and dst can either be strings or (Fs,Remote) tuples. The latter will
        optimize the calls since it will cache more information
        """
        endpoint = f"operations/{cpmv}"
        method = self.call_async_and_wait if use_async else self.call

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
        method = self.call_async_and_wait if use_async else self.call

        endpoint = "operations/deletefile"
        params["fs"], params["remote"] = rcpathsplit(file)

        return method(endpoint, params=params)

    def write(self, dst, content, use_async=False, **params):
        endpoint = "operations/uploadfile"
        method = self.call_async_and_wait if use_async else self.call

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
            logger.debug("Trying write fallback")
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

    def _http_head(self, src):
        self.start()

        fs, file = rcpathsplit(src)
        file_url = urllib.parse.urljoin(
            f"http://{self.user}:{self.password}@{self.addr}",
            f"[{fs}]/{file}",
        )
        res = requests.head(file_url)
        return res.headers

    def read(self, src, start=0, end=None):
        """
        Read directly from the remote. This is like "rclone cat"

        Inputs:
        -------

        src
            Either a remote path or (fs,remote/file) tuple

        start [0]
            Starting range

        end [None]
            End range

        Note on start,end
        ----------------
        Ranges are passed directly to as headers and are INCLUSIVE. (i.e. end=255
        is the first 256 bytes). Specify as None to be excluded. For example,

            | start | end  | comment                    |
            |-------|------|----------------------------|
            | 0     | None | Read all                   |
            | 256   | None | Read all but the first 256 |
            | None  | 300  | read the *last* 300 bytes  |

        See https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Range

        If there is a failure, just download the file and read it. But need to be
        careful of ranges
        """
        self.start()

        fs, file = rcpathsplit(src)
        file_url = urllib.parse.urljoin(
            f"http://{self.user}:{self.password}@{self.addr}",
            f"[{fs}]/{file}",
        )

        if start is None:
            start = ""
        if end is None:
            end = ""

        res = requests.get(file_url, headers={"Range": f"bytes={start}-{end}"})
        if res.status_code == 404:
            raise ValueError("Not Found or range too far")
        return res.content

    def open(
        self,
        remotefile,
        mode="rb",
        buffer_size=8 * 1024 * 1024,
    ):
        """
        Return a buffered, seekable, READ-ONLY, file-like object of 'remotefile',
        that is buffered with 'buff_size'. Default buffer_size is 8 MiB or
        8*1024*1024 = 8388608 bytes

        mode:
            Only supports reading so 'r','rt','rb'

        buffer_size [8388608]
            Amount to buffer. Default: 8 MiB = 8*1024*1024 bytes = 8388608 bytes

        """
        if "w" in mode or "a" in mode:
            raise io.UnsupportedOperation("Cannot open in write or append mode")
        raw = _RawRcloneFileObj(remotefile, self)
        fp = io.BufferedReader(raw, buffer_size=buffer_size)
        if "b" not in mode:
            fp = io.TextIOWrapper(fp)

        for attr in ["remotefile", "fs", "remote"]:
            if val := getattr(raw, attr, None):
                setattr(fp, attr, val)
        fp._is_rc_file = True  # used in rcpathsplit

        return fp

    def list(
        self,
        remote,
        *,
        recurse=True,
        filters=None,
        filter_params=None,
        filter_cli="",
        mimetype=False,
        modtime=True,
        hashes=False,
        hashtypes=False,
        metadata=False,
        maxdepth=None,
        only=None,
        epoch_time=False,
        fast_list=True,
        config_params=None,
        use_async=False,
        **params,
    ):
        """
        List remotes. Note that most filters are via filter_params or can be converted
        from the CLI flags with filter_cli. The only exception is the 'filters' keyword
        which gets appended to filter_params['FilterRule']

        Inputs:
        -------
        remote
            Either (Fs,Remote) tuple or a a single string which will be split

        recurse [True]
            Whether to recursively list

        filters:
            Convenience for filter_params['FilterRule']. Everything else is via
            filter_params! List or string

        filter_params [empty]
            Filters for _filter in the rc call. An incomplete list includes:

                    'FilterRule' 'FilterFrom' 'FilesFrom' 'FilesFromRaw'
                    'ExcludeRule' 'ExcludeFrom' 'IncludeRule' 'IncludeFrom' 'ExcludeFile'
                    'MinAge' 'MaxAge' 'MinSize' 'MaxSize'
                    'DeleteExcluded' 'IgnoreCase'

            See 'options/get' for full list and see filter_cli keyword to accept command
            line flags.

            References: https://rclone.org/rc/#setting-filter-flags-with-filter

        filter_cli [None]
            Convenience for filter_params = filter_cli2params(filter_cli). Can be
            a list of a string

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
            If True, equivalent of --metadata

        maxdepth [None]
            Adds equiv of --max-depth flags.

        only [None]
            Specify as {None [default],'files','dirs'}.

        epoch_time [False]
            If True, returns a Unix Epoch float. Otherwise, returns a timezone aware
            datetime object

            These can be set here and/or in the

        fast_list [True]
            Whether or not to use --fast-list. If being recursive, this is probably
            better

        config_params [empty]
           Additional _config dictionary. See options/get

        **params
            Passed to the call(). Use config_params to set stat params
        """
        # Note: Everthing is build into the params keyword argument.
        # So we pop from it as needed for other settings

        # Build the call. First params
        params["fs"], params["remote"] = rcpathsplit(remote)
        params["remote"] = params["remote"].removesuffix("/")

        # Filters. Allow for _filter in **params, FilterRule, filter_params,
        # and filter_cli (in that order) with extra care taken for MetaRules
        filter_params = params.get("_filter", {}) | dictify(filter_params)

        if f := listify(filters):
            filter_params["FilterRule"] = filter_params.get("FilterRule", []) + f

        meta_filters = filter_params.pop("MetaRules", {})

        cli = filter_cli2params(filter_cli)
        meta_filters |= cli.pop("MetaRules", {})

        filter_params |= cli
        if meta_filters:
            filter_params["MetaRules"] = meta_filters

        params["_filter"] = filter_params

        # Other config
        params["_config"] = params.get("_config", {}) | dictify(config_params)

        # Rest of the KWs
        opt = params["opt"] = params.get("opt", {})
        opt["recurse"] = recurse
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

        params["_config"]["UseListR"] = fast_list

        method = self.call_async_and_wait if use_async else self.call

        res = method("operations/list", params=params)
        if "list" not in res:
            raise ValueError(f"Error listing remote. Check keywords. {res = }")
        for line in res["list"]:
            # Never understood why rclone gives us this...
            line.pop("Name", None)

            if "ModTime" in line:  # Do regardless of modtime setting
                line["ModTime"] = timestamp_parser(line["ModTime"], epoch=epoch_time)
        return res["list"]

    def stat(
        self,
        remotefile,
        *,
        mimetype=False,
        modtime=True,
        hashes=False,
        hashtypes=None,
        metadata=False,
        epoch_time=False,
        config_params=None,
        use_async=False,
        **params,
    ):
        """
        remotefile
            Either a string path to the file or (fs,file) tuple

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
            If True, equivalent of --metadata

        epoch_time [False]
            If True, returns a Unix Epoch float. Otherwise, returns a timezone aware
            datetime object

        config_params [empty]
           Additional _config dictionary. See options/get

        **params
            Passed to the call(). Use config_params to set stat params
        """
        params["fs"], params["remote"] = rcpathsplit(remotefile)

        params["_config"] = params.get("_config", {}) | dictify(config_params)
        params["opt"] = opt = params.get("opt", {})

        opt["filesOnly"] = True
        opt["noMimeType"] = not mimetype
        opt["noModTime"] = not modtime
        opt["showHash"] = hashes
        if hashtypes and hashes:
            opt["hashTypes"] = listify(hashtypes)
        opt["metadata"] = metadata

        method = self.call_async_and_wait if use_async else self.call

        item = self.call("operations/stat", params=params).get("item", None)
        if not item:
            return

        item.pop("Name", None)
        if "ModTime" in item:  # Do regardless of modtime setting
            item["ModTime"] = timestamp_parser(item["ModTime"], epoch=epoch_time)

        return item

    @cache
    def features(self, fs, **params):
        params["fs"] = fs
        return self.call("operations/fsinfo", params=params)

    def call(self, endpoint, *, postkw=None, params=None, **paramskwargs):
        self.start()

        postkw = postkw or {}
        params = paramskwargs | (params or {})

        logging.debug(f"call: {endpoint = }, {params = }")

        for key, val in params.items():
            if isinstance(val, (dict, list)):
                params[key] = json.dumps(val)

        # In order to get sending data for rcat (aka write) to work, we use the URL
        # paramaters and post anything else as data. This makes the URLs more cumbersome
        # but in my testing, works better since you can post content.
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

        # This is developer-level debug. Comment out for now
        # logger.debug(f"call {res = }")

        if res.get("error", ""):
            err = RcloneError(f"Error. Result: {res}")
            err.response = res
            raise err
        return res

    def call_async_and_background(
        self, endpoint, postkw=None, params=None, **paramskwargs
    ):
        """Call with async and return jobid"""
        params = paramskwargs | (params or {})
        params = params.copy()
        params["_async"] = True
        jobid = self.call(endpoint, postkw=postkw, params=params)["jobid"]
        return jobid

    def check_async(self, jobid):
        """Check on async job. return none if not done"""
        res = self.call("job/status", params={"jobid": jobid})
        if res.get("finished", False) or res.get("error", ""):
            return res

    def call_async_and_wait(self, endpoint, postkw=None, params=None, **paramskwargs):
        """
        This is basically the same as call() but uses async. Calls and waits
        for return
        """
        jobid = self.call_async_and_background(
            endpoint, postkw=postkw, params=params, **paramskwargs
        )
        t0 = time.time()

        while True:
            elapsed = time.time() - t0
            dt = dtfun(elapsed)
            if res := self.check_async(jobid):
                break
            time.sleep(dt)
        return res


class _RawRcloneFileObj(io.RawIOBase):
    # PRIVATE. Use RC.open() for a buffered one
    # We could return the .raw from requests but this is seekable and
    # we more closely control the requests from rclone

    def __init__(self, remotefile, rc):
        self.rc = rc
        self.remotefile = (self.fs, self.remote) = rcpathsplit(remotefile)
        self.offset = 0

        self._head = head = rc._http_head(self.remotefile)
        if mx := head.get("Content-Length", None):
            self.maxsize = int(mx)
        else:
            self.maxsize = None

    def seekable(self):
        return True

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            self.offset = offset
        elif whence == io.SEEK_CUR:
            self.offset += offset
        elif whence == io.SEEK_END:
            if self.maxsize is None:
                raise io.UnsupportedOperation("Could not determine size")
            self.offset = self.maxsize + offset
        else:
            raise io.UnsupportedOperation()
        return self.offset

    def tell(self):
        return self.offset

    def readable(self):
        return True

    def writable(self):
        return False

    def read(self, size=-1, /):
        # Handle when reading everything since this is a lot faster
        if size >= 0:
            return super().read(size)

        if self.maxsize and self.offset >= self.maxsize:
            return b""

        return self.rc.read(
            self.remotefile,
            start=self.offset,
            end=None,
        )

    def readall(self):
        return self.read(-1)

    def readinto(self, b):
        if self.maxsize and self.offset >= self.maxsize:
            return 0

        N = len(b)
        try:
            chunk = self.rc.read(
                self.remotefile,
                start=self.offset,
                end=self.offset + N - 1,  # -1 since end is inclusive
            )
        except ValueError:
            b.clear()
            return 0

        n = len(chunk)
        self.offset += n
        if n != N:  # We know we hit the end since it returned less than we wanted
            self.maxsize = self.offset
        b[:n] = chunk
        return n


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

    The algorithim is heuristic but should account for most cases. It removes the
    leading ':' if present (from an on-the-fly remote). Then it removes everything that
    is quoted. Afterwards, it splits at : and replaces the removed values. It does not
    require quotes to be matched anywhere but in the remote name but that is also an
    rclone requirement
    """
    if isinstance(path, (tuple, list)) and len(path) == 2:
        return path

    if getattr(path, "_is_rc_file", False):
        return path.remotefile

    path = str(path)

    if otf := path.startswith(":"):  # on the fly
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


def rcpathjoin(*args):
    """
    This is like os.path.join but does some rclone-specific things because
    there could be a ':' in the first part.

    The second argument could be '/file', or 'file' and the first could have a colon.
        RcloneCLI.pathjoin('a','b')   # a/b
        RcloneCLI.pathjoin('a:','b')  # a:b
        RcloneCLI.pathjoin('a:','/b') # a:/b # Note that these are unlike os.path.join
        RcloneCLI.pathjoin('a','/b')  # a/b

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


def filter_cli2params(flags):
    """
    Converts and maps rclone flags into a _filter dict for rclone rc calls.

    Arguments:
        flags
            List or string of flags. If string, will apply shlex.split on them

    Returns:
        _filter dictionary

    Valid Flags:
        --delete-excluded
        --filter --filter-from
        --exclude --exclude-from
        --include --include-from
        --exclude-if-present
        --files-from --files-from-raw
        --min-age --max-age
        --min-size --max-size
        --ignore-case
        --metadata-filter --metadata-filter-from
        --metadata-exclude --metadata-exclude-from
        --metadata-include --metadata-include-from
    """
    if isinstance(flags, str):
        flags = shlex.split(flags)

    param2flag = {
        "DeleteExcluded": "--delete-excluded",
        "FilterRule": "--filter",
        "FilterFrom": "--filter-from",
        "ExcludeRule": "--exclude",
        "ExcludeFrom": "--exclude-from",
        "IncludeRule": "--include",
        "IncludeFrom": "--include-from",
        "ExcludeFile": "--exclude-if-present",
        "FilesFrom": "--files-from",
        "FilesFromRaw": "--files-from-raw",
        "MinAge": "--min-age",
        "MaxAge": "--max-age",
        "MinSize": "--min-size",
        "MaxSize": "--max-size",
        "IgnoreCase": "--ignore-case",
        "meta_FilterRule": "--metadata-filter",
        "meta_FilterFrom": "--metadata-filter-from",
        "meta_ExcludeRule": "--metadata-exclude",
        "meta_ExcludeFrom": "--metadata-exclude-from",
        "meta_IncludeRule": "--metadata-include",
        "meta_IncludeFrom": "--metadata-include-from",
    }

    BOOL_PARAMS = {"IgnoreCase", "DeleteExcluded"}
    SINGLE_PARAMS = {"MinAge", "MaxAge", "MinSize", "MaxSize"}

    parser = ThrowingArgumentParser()
    for param, flag in param2flag.items():
        if param in BOOL_PARAMS:
            parser.add_argument(flag, dest=param, action="store_true", default=None)
        elif param in SINGLE_PARAMS:
            parser.add_argument(flag, dest=param, default=None)
        else:
            parser.add_argument(
                flag,
                dest=param,
                action="append",
                default=None,
                metavar="VAL",
            )

    args = parser.parse_args(flags)

    params = {}
    meta_params = {}

    for param in param2flag:
        if not (val := getattr(args, param)):
            continue

        if param.startswith("meta_"):
            meta_params[param.removeprefix("meta_")] = val
        else:
            params[param] = val

    if meta_params:
        params["MetaRules"] = meta_params

    return params


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
        "--delete-excluded",
        "--exclude",
        "--exclude-from",
        "--exclude-if-present",
        "--files-from",
        "--files-from-raw",
        "--filter",
        "--filter-from",
        "--ignore-case",
        "--include",
        "--include-from",
        "--max-age",
        "--max-size",
        "--metadata-exclude",
        "--metadata-exclude-from",
        "--metadata-filter",
        "--metadata-filter-from",
        "--metadata-include",
        "--metadata-include-from",
        "--min-age",
        "--min-size",
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
