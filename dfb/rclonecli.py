"""
This will eventually be its own module. But for now, it is just bundled with dfb...

rclone interfacing as a cloud API. This is intended to be like an API for a cloud 
interface and less to mimic rclone's synchronizing. As such, most method default to
always performing transfers. (via `--no-check-dest` though can change)

It is assumed that this is used by someone who is familiar with rclone and some of its
idiosyncrasies. Most methods have the ability to specify additional flags and/or
calling options. There are no guardrails so things could break! It has some built in 
methods and you can do a low-level `rclone.call()`.

It is a single file to make integrating with other codes easier.

It was started as a proof-of-concept that grew. It is still a work in progress with
some more to be added.

**TODO**: Capture failures. Especially around streaming uploads but really everywhere  
**TODO**: Better documentation of paramaters  
**TODO**: More of the example for conditional uploads which remove the always flags
**TODO**: Type annotation

NOTE: rclonerc is a better way to use rclone. Many of these features predate me using
      that API
"""

import io
import json
import logging
import os
import string
import subprocess
import sys
import tempfile
import time
import types
import warnings
from functools import cached_property, partial, partialmethod

from . import __version__
from .timestamps import timestamp_parser

logger = logging.getLogger(__name__)


class RcloneCLI:
    """
    RcloneCLI object.

    Inputs:
    -------
    remote
        String remote name.

    rclone_exe ['rclone']
        Executable to call

    universal_flags ( empty tuple )
        Flags to always include. Could be thinks like config specification

    universal_env (None / Empty dict)
        Environment to awlays include. This is appended to os.environ.
        If there is something in os.environ that is *not* wanted, specify it
        with RLCONE.DELENV. ex:
            {"RCLONE_PASSWORD_COMMAND": RcloneCLI.DELENV}

    no_check_dest [True]
        On many upload operations, it is faster to include '--no-check-dest' but on
        some that can have duplicate files, such as Google Drive, set this to False
        and will use '--ignore-times' instead.

    Notes:
    -----
    - There are minimal guardrails. It is possible to specify conflicting flags.
      It is expected that you have some idea of what you're doing.

    - The general philospogy on flags is to make them keyword arguments for the most
     important or common ones.

    - Most methods have a 'flags' keyword to specify additional flags. As noted, there
      are no guard rails on them. Most also have callopts which let you control how
      rclone is called. This may be used to enable streaming the output.

    - This is designed with single files in mind. There are some methods that would be
      better off done with their own direct calls. Or this can be subclassed.

    - By design, all uploads and downloads will always happen unless wrapped with the
      'not_always' context manager.

    - Anytime an RcloneFile object is passed as a source, it's path is used. This enables
      remote-to-remote transfers. Note that the flag and environment of the main object
      is used. This means that, for example, you cannot have remotes defined in their own
      config files.


    Useful Flags:
    -------------
    It may be useful to set '--links', '--copy-links', or '--skip-links' but they are not
    otherwise set

    """

    NO_TRAVERSE = 50  # WAG of a cutoff for multiple uploads
    BLOCKSIZE = 1024  # 1kb. For reading inputs

    # Sentinels. Made with os.urandom(8)
    DELENV = "**DELENV**"  # Remove from environment
    NOFLAG = "XTfRDcsimw"  # Remove from call

    def __init__(
        self,
        remote,
        *,
        rclone_exe="rclone",
        universal_flags=None,
        universal_env=None,
        no_check_dest=True,
    ):
        self.remote = remote
        self.rclone_exe = rclone_exe
        self.uflags = [str(flag) for flag in _flagify(universal_flags)]

        # Save this for presenting
        self.universal_env = universal_env = {
            str(k): str(v) for k, v in _dictify(universal_env).items()
        }

        # But this is for saved for real use
        self.uenv = os.environ.copy()
        self.uenv.update(universal_env)
        for key, val in universal_env.items():
            if val == self.DELENV:
                self.uenv.pop(key, None)
                universal_env[key] = "**UNSET**"

        self.always_flag = "--no-check-dest" if no_check_dest else "--ignore-times"

        self._capture = False

    # Main Actions. All also contains flag and callopts arguments for more control

    def listremote(
        self,
        subdir="",
        *,
        filters=None,
        filter_flags=None,
        fast_list=False,
        mimetype=False,
        modtime=True,
        hashes=False,
        hashtypes=False,
        metadata=False,
        maxdepth=None,
        only=None,
        epoch_time=False,
        # pipe=False,
        flags=None,
        callopts=None,
    ):
        """
        List the remote with lsjson.

        Some common flags are built in options for convenience but any additional
        can be specified manually. See https://rclone.org/commands/rclone_lsjson/

        The only processing is to convert ModTime to Python datetime objects

        Inputs:
        -------
        subdir [empty]
            Subdir to list. Note that paths will be relative TO THIS DIR.
            You can use os.path.join on the output if needed.

            Can also be a specific file but will still return an iterator.
                >>> fileinfo = list(rclone.listremote(subdir='myfile.ext'))[0]

        filters [empty]
            RcloneCLI filters as items. See also filter_flags

        filter_flags [empty]
            Additional filtering flags such as
                --include, --exclude, --include-from, --exclude-from,
                --filter, --filter-from, --files-from,
                --one-file-system, --exclude-if-present

            This is effectively the same as flags but more explicit.

        fast_list [False]
            Whether to add --fast-list. Or specify 'auto' to be True if
            the remote supports it.

        mimetype [False]
            If False, will call with --no-mimetype

        modtime [True]
            If False, will call with --no-modtime. This can be faster on remotes
            that require additional requests for it. Note that it still may or may not
            include a ModTime. ModTimes are converted to datetime objects unless
            epoch_time=True.

        hashes [False]
            Compute hashes (adds '--hash')

        hashtypes [None]
            If specified and hashes=True, will use these types. String of one or list
            of types

        metadata [False]
            If True, adds --metadata

        maxdepth [None]
            Adds --max-depth flags.

        only
            Specify as {None [default],'files','dirs'}.

        epoch_time [False]
            If True, returns a Unix Epoch float. Otherwise, returns a time zone aware
            datetime object

        flags [empty]
            Additional rclone flags. No guardrails or error checking!

        callopts [empty]
            Additional options passed to call. No guardrails or error checking!

        Returns:
        -------
        Iterator on file items

        """
        """
        pipe [False]
            Whether to write output to an intermediate file.
        """
        pipe = True  # TODO...

        subdir = subdir or ""  # Convert None to ""

        cmd = ["lsjson", RcloneCLI.pathjoin(self.remote, subdir), "--recursive"]
        if fast_list == "auto":
            fast_list = rclone.features.get("ListR", False)
        if fast_list:
            cmd.append("--fast-list")

        for ff in _flagify(filters):
            cmd.append("--filter")
            cmd.append(ff)
        cmd += _flagify(filter_flags)

        if not mimetype:
            cmd.append("--no-mimetype")
        if not modtime:
            cmd.append("--no-modtime")
        if hashes:
            cmd.append("--hash"),
            if hashtypes:  # Only if hashes
                hashtypes = _flagify(hashtypes)
                for hashtype in hashtypes:
                    cmd.extend(["--hash-type", hashtype])
        if metadata:
            cmd.append("--metadata")
        if maxdepth:
            cmd.extend(["--max-depth", str(maxdepth)])

        if only not in {None, "dirs", "files"}:
            raise ValueError("'only' must be one of {None,'dirs','files'}")
        if only:
            cmd.append(f"--{only}-only")

        cmd += _flagify(flags)

        # Long directories can be problematic so use a tempfile so as to not exhaust the
        # buffer. We also want to parse this lazily since it could be long. This isn't
        # perfect since we wait for the entire listing to finish then read line by line
        # but it avoids accidentally deadlocking.
        res = self.call(cmd, pipe=pipe, stream=True, **_dictify(callopts))

        # Special case for '--stat' whether user specified or from iteminfo.
        # RcloneCLI doesn't do the one-line-per-response with this call.
        # A bit of a hack but we want it to pass through the for loop processing still.
        if "--stat" in cmd:
            lines = []
            for oe, line in res:
                if oe != "stdout":
                    continue
                lines.append(line)
            item = json.loads(b"".join(lines))
            res = [("stdout", json.dumps(item).encode())]

        for oe, line in res:
            if oe != "stdout":
                logger.debug(f"stdout: {line}")
                continue

            # lsjson returns one entry per line. And always UTF8
            line = line.decode("utf8")
            line = line.strip().rstrip(",").strip()

            if line == "[" or line == "]":  # start or end line
                continue

            line = json.loads(line)

            # Never understood why rclone gives us this...
            line.pop("Name", None)

            if "ModTime" in line:  # Do regardless of modtime setting
                line["ModTime"] = timestamp_parser(line["ModTime"], epoch=epoch_time)
            yield line

    ls = listremote

    def iteminfo(self, remoteitem, **kwargs):
        """
        List a single item. This is a convenience function around listremote that just
        returns the first item.

        Inputs:
        -------
        remoteitem
            Remote item of interest. If empty string, will be the root directory of
            the remote

        **kwargs
            Passed to listremote

        """
        kwargs["flags"] = _flagify(kwargs.get("flags", None)) + ["--stat"]
        kwargs["subdir"] = remoteitem

        info = list(self.listremote(**kwargs))
        return info[0]

    def upload(self, local, destdir="", *, flags=None, callopts=None):
        """
        Upload a single file 'local' to the 'destdir' keeping the name.

        Inputs:
        -------
        local
            Local file to upload

        destdir ['']
            Where on the remote to upload

        flags [empty]
            Additional rclone flags. No guardrails or error checking!

        callopts [empty]
            Additional options passed to call. No guardrails or error checking!
            Useful ones are 'return_proc' and 'stream'

        Returns:
        -------
        results of .call(). Usually stdout,stderr unless callpopts were changed
        """
        cmd = [
            "copy",
            local,
            RcloneCLI.pathjoin(self.remote, destdir),
            "--no-traverse",  # Single file. This is better
            self.always_flag,  # Always upload
        ] + _flagify(flags)

        return self.call(cmd, **_dictify(callopts))

    def uploadto(self, local, destfile, *, flags=None, callopts=None):
        """
        Upload a single file 'local' to the 'destfile' with any name

        Inputs:
        -------
        local
            Local file to upload

        destfile ['']
            Destination file name

        flags [empty]
            Additional rclone flags. No guardrails or error checking!

        callopts [empty]
            Additional options passed to call. No guardrails or error checking!
            Useful ones are 'return_proc' and 'stream'

        Returns:
        -------
        results of .call(). Usually stdout,stderr unless callpopts were changed
        """
        cmd = [
            "copyto",
            local,
            RcloneCLI.pathjoin(self.remote, destfile),
            "--no-traverse",  # Single file. This is better
            self.always_flag,  # Always upload
        ] + _flagify(flags)

        return self.call(cmd, **_dictify(callopts))

    def uploadmany(
        self,
        files,
        destdir="",
        upload_from=".",
        *,
        flags=None,
        callopts=None,
    ):
        """
        Upload many files from 'upload_from' location. Can combine with the not_always
        context manager.

        Inputs:
        ------
        files
            List of file paths. They should be relative to 'upload_from'

        destdir ['']
            Where on the remote to upload

        upload_from ['.']
            The local "remote"

        flags [empty]
            Additional rclone flags. No guardrails or error checking!

        callopts [empty]
            Additional options passed to call. No guardrails or error checking!
            Useful ones are 'return_proc' and 'stream'

        Returns:
        -------
        results of .call(). Usually stdout,stderr unless callpopts were changed
        """
        if isinstance(files, str):
            files = [files]
        files = list(files)

        with tempfile.NamedTemporaryFile(delete=False, mode="wt") as fp:
            for file in files:
                if file.startswith(".."):
                    msg = f"Will not upload files above specified location. {file!r}"
                    warnings.warn(msg)
                print(file, file=fp)

        cmd = [
            "copy",
            upload_from,
            RcloneCLI.pathjoin(self.remote, destdir),
            self.always_flag,  # Always upload
            "--files-from",
            fp.name,
        ] + _flagify(flags)

        # I am not sure about this one. I think that if you are always uploading, it
        # doesn't matter. Or maybe just for no-check-dest. In which case, this is
        # probably implied anyway.
        if len(files) < self.NO_TRAVERSE:
            cmd += ["--no-traverse"]

        return self.call(cmd, **_dictify(callopts))

    def write(
        self,
        data,
        destfile,
        *,
        size=None,
        fallback=False,
        flags=None,
        callopts=None,
    ):
        """
        Upload data to a file

        Inputs:
        -------
        data
            Data to upload. If a string, it will be UTF8 encoded to bytes.
            If it a file object, it will be read in RcloneCLI.BLOCKSIZE blocks
            and uploaded. If a types.GeneratorType, will iterates and sent
            to rclone

        destfile ['']
            Destination file name

        size [None]
            Specify the size of it is known. This will enable more backends to
            directly support uplaods. If given a string or bytes, this will
            be automatically calculated

        fallback [False]
            Fallback to writing a temp file and uploading. Can also set to 'auto'

        flags [empty]
            Additional rclone flags. No guardrails or error checking!

        callopts [empty]
            Additional options passed to call. No guardrails or error checking!
            Useful ones are 'return_proc' and 'stream'

        Returns:
        -------
        results of .call(). Usually stdout,stderr unless callpopts were changed

        """
        if fallback == "auto":
            opts = dict(size=size, flags=flags, callopts=callopts)
            try:
                return self.write(data, destfile, fallback=False, **opts)
            except:  # Allow it to be anything!
                logger.error("write failed. Trying fallback")
                return self.write(data, destfile, fallback=True, **opts)

        ###########################

        if isinstance(data, str):
            data = data.encode("utf8")

        if fallback:
            with tempfile.NamedTemporaryFile() as fp:
                if hasattr(data, "read"):
                    while block := data.read(self.BLOCKSIZE):
                        fp.write(block)
                elif isinstance(data, types.GeneratorType):
                    for block in data:
                        fp.write(block)
                elif isinstance(data, bytes):
                    fp.write(data)
                else:
                    raise TypeError("Could not handle input")
                fp.flush()
                return self.uploadto(fp.name, destfile, flags=flags, callopts=callopts)

        if isinstance(data, bytes):
            size = len(data)

        cmd = [
            "rcat",
            RcloneCLI.pathjoin(self.remote, destfile),
            "--no-traverse",  # Single file. This is better
            self.always_flag,  # Always upload
        ] + _flagify(flags)
        if size:
            cmd.extend(["--size", str(size)])

        return self.call(cmd, stdin=data, **_dictify(callopts))

    def download(self, remotefile, destdir, *, flags=None, callopts=None):
        """downloads a remotefile file to the destdir"""
        cmd = [
            "copy",
            RcloneCLI.pathjoin(self.remote, remotefile),
            destdir,
            "--no-traverse",  # Single file. This is better
            self.always_flag,  # Always download
        ] + _flagify(flags)

        return self.call(cmd, **_dictify(callopts))

    def downloadto(self, remotefile, destfile, *, flags=None, callopts=None):
        """Upload a local file to the destfile"""
        cmd = [
            "copyto",
            RcloneCLI.pathjoin(self.remote, remotefile),
            destfile,
            "--no-traverse",  # Single file. This is better
            self.always_flag,  # Always download
        ] + _flagify(flags)

        return self.call(cmd, **_dictify(callopts))

    def read(
        self,
        remotefile,
        *,
        offset=None,
        count=None,
        fileobject=False,
        flags=None,
        callopts=None,
    ):
        """
        Return the bytes of the file or a fileobject. Note that changing the
        pipe settings with callopts may require you to seek() the file.
        """
        cmd = ["cat", RcloneCLI.pathjoin(self.remote, remotefile)]
        if offset:
            cmd.extend(["--offset", str(offset)])
        if count:
            cmd.extend(["--count", str(count)])

        cmd += _flagify(flags)

        kw = _dictify(callopts)

        if fileobject:
            kw["return_proc"] = True
            res = self.call(cmd, **kw).stdout
        else:
            kw["return_proc"] = False
            kw["pipe"] = True
            res = self.call(cmd, **kw)

        if isinstance(res, tuple):  # default
            return res[0]
        return res  # Maybe a callopts set it to the proc

    def delete(
        self,
        remotefile,
        *,
        filters=None,
        rmdirs=False,
        flags=None,
        callopts=None,
    ):
        """
        Delete a remote path, potentially including directories.
        """

        cmd = ["delete", RcloneCLI.pathjoin(self.remote, remotefile)] + _flagify(flags)

        if rmdirs:
            cmd.append("--rmdirs")

        for ff in _flagify(filters):
            cmd.append("--filter")
            cmd.append(ff)

        return self.call(cmd, **_dictify(callopts))

    def open(
        self,
        remotefile,
        mode="rb",
        buffer_size=8 * 1024 * 1024,
        flags=None,
        callopts=None,
    ):
        """
        Return a buffered, seekable, file-like object of 'remotefile',
        that is buffered with 'buff_size'. Default buffer_size is 8mb or
        8*1024*1024 = 8388608 bytes

        Warning though, if called with .read(), will use a *much* smaller
        buffer. It will work but can be extremely slow!

        """
        if "w" in mode:
            raise io.UnsupportedOperation("Cannot write to stream")
        raw = _RawRcloneFileObj(remotefile, self, flags=flags, callopts=callopts)
        fp = io.BufferedReader(raw, buffer_size=buffer_size)
        if "b" in mode:
            return fp
        return io.TextIOWrapper(fp)

    def _movecopy(self, remotesrc, remotedst, *, _cmd, flags=None, callopts=None):
        cmd = [
            _cmd,
            RcloneCLI.pathjoin(self.remote, remotesrc),
            RcloneCLI.pathjoin(self.remote, remotedst),
            "--no-traverse",  # Single file. This is better
            self.always_flag,  # Always
        ] + _flagify(flags)

        return self.call(cmd, **_dictify(callopts))

    move = partialmethod(_movecopy, _cmd="move")
    copy = partialmethod(_movecopy, _cmd="copy")
    moveto = partialmethod(_movecopy, _cmd="moveto")
    copyto = partialmethod(_movecopy, _cmd="copyto")

    @cached_property
    def version(self):
        cmd = ["version"]
        res, _ = self.call(cmd)
        return res

    @cached_property
    def version_dict(self):
        cmd = ["rc", "--loopback", "core/version"]
        res, _ = self.call(cmd)
        return json.loads(res)

    @cached_property
    def backend_features(self):
        cmd = ["backend", "features", self.remote]
        res, _ = self.call(cmd)
        return json.loads(res)

    @property
    def features(self):
        return self.backend_features.get("Features", {})

    @cached_property
    def config_paths(self):
        out, _ = self.call(["config", "paths"])
        paths = {}
        for line in out.decode().splitlines():
            line = line.strip()
            if not line:
                continue
            key, val = line.split(":", 2)
            paths[key.strip()] = val.strip()
        return paths

    def call(
        self,
        cmd,
        *,
        stdin=None,
        pipe=True,
        stream=False,
        allow_error=False,
        return_proc=False,
    ):
        """
        Call rclone and get results, or Popen object, or iterator over lines

        Options:
        -------
        stdin [None]
            What to send to rclone. If bytes, will send directly. If a file
            object, will send in RcloneCLI.BLOCKSIZE (default 1kb) chunks. If a
            types.GeneratorType, will iterate and sent to rclone

        pipe [True]
            Whether or not to use subprocess.PIPE (buffered in memory) or write
            to a temp file. Even if false, will still work with stream.

        stream: [False]
            Return a generator of (stdin -or- stdout,line) pairs that can be printed
            to the user right away. Sets return_proc=False but will still works with
            either pipe setting. If pipe is False, raises an error.

            Note that streaming output will not start until stdin has been fully written.
            It is possible to work around this limitation but not worth the added
            complexity.

        allow_error [False]
            If True, does not raise an error on failure. Ignored for return_proc
            but not for stream.

        return_proc: [False]
            If True, will return the Popen object rather than call communicate.
            It will always have stdin,stdout variables defined even but with buffer=False,
            they will need to be seeked to the start or should be open as a new reader.



        """
        finalcmd = [self.rclone_exe] + self.uflags + cmd
        finalcmd = [c for c in finalcmd if c != self.NOFLAG]
        finalcmd = [c if not isinstance(c, RcloneFile) else c.path for c in finalcmd]

        logger.debug(
            f"rclone call {str(finalcmd)} with additional "
            f"environ {json.dumps(self.universal_env)}"
        )

        if pipe:
            stdout = subprocess.PIPE
            stderr = subprocess.PIPE
        else:
            if stream:
                raise ValueError("Cannot stream without pipe=True")
            stdout = tempfile.NamedTemporaryFile(delete=False)
            stderr = tempfile.NamedTemporaryFile(delete=False)

        # Include this here so we can replace it in either a subclass or monkey-patch
        proc = self._Popen(
            finalcmd,
            stdout=stdout,
            stderr=stderr,
            stdin=subprocess.PIPE,
            env=self.uenv,
        )
        if not proc:
            return

        if not pipe:
            proc.stdout, proc.stderr = stdout, stderr

        if hasattr(stdin, "read"):
            # Do it this ways instead of setting this as stdin
            # since it doesn't require stdin to be a real file.
            while block := stdin.read(self.BLOCKSIZE):
                proc.stdin.write(block)
        elif isinstance(stdin, types.GeneratorType):
            for block in stdin:
                proc.stdin.write(block)
        elif stdin:
            # Do it here rather than in communicate in case we are returning ot
            proc.stdin.write(stdin)

        if stream:
            # Technically, the stream is still imperfect since it won't start streaming
            # until we've written all of stdin. It could be done via threads but the
            # added complexity is not worth it. And it will be *much* harder to ensure
            # no deadlocks. So we write all of stdin before we start reading stdout.
            # This is noted above
            proc.stdin.close()

            return popen_streamer(proc, allow_error=allow_error)

        if return_proc:
            proc.stdin.close()
            return proc

        out, err = proc.communicate()  # Also does proc.wait()

        if not allow_error:
            check_returncode(proc, output=out, stderr=err)

        if not pipe:
            return stdout, stderr

        return out, err

    def _Popen(self, *args, **kwargs):
        return subprocess.Popen(*args, close_fds=True, **kwargs)

    # def logger.debug(self, *args, **kwargs):
    #         """Debug. Can subclass as needed"""
    #         return logger.logger.debug(*args, **kwargs)

    @staticmethod
    def pathjoin(*args, local_root=False):
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
            return "".pathjoin(args)

        root, first, rest = args[0], args[1], args[2:]

        if root.endswith("/"):
            root = root[:-1]

        if root.endswith(":") or first.startswith("/"):
            path = root + first
        else:
            path = f"{root}/{first}"

        path = os.path.join(path, *rest)
        return path

    @property
    def not_always(self, **kwargs):
        """
        Context manager to disable unconditional uploads. Useful if you
        do not want to force an upload if rclone thinks they are the same.

        Note that you may want to add additional flags to control it.

        Examples:

        Normal. Will always upload

            >>> rclone.upload('existing_file.ext')

        Use default checks:

            >>> with rclone.not_always:
            >>>     rclone.upload('existing_file.ext')

        Use '--checksum' mode:

            >>> with rclone.not_always:
            >>>     rclone.upload('existing_file.ext',flags=['--checksum'])
        """
        return enable_check_dest(self, **kwargs)

    enable_check_dest = not_always

    def capture(self, execute=True, save_results=False, save_stdin=False):
        """
        Context Manager to capture the subprocess command and optionally, the results.

        Can also tell it not to execute but this may cause issues downstream.

        Example:

            IN: rclone = RcloneCLI('myremote:')
                with rclone.capture(save_results=True) as capture:
                    rclone.write('test data','data.txt')
                print(capture.command_history[0])
                print(capture.command_results[0])

           OUT: ['rclone', 'rcat', 'myremote:data.txt',
                 '--no-traverse', '--no-check-dest', '--size', '9']

                 (b'', b'') # No output but captured

        Note that you can use rclone.universal_env to get the *specified* environment
        or rclone.uenv to get the *actual* used environment

        Again, execute=False may cause downstream failure.

        If using save_stdin, it saves bytes or strings. If stdin is specified as
        something else, it will be b'**not bytes**'. Otherwise, it's None.

        Warning: Not thread-safe. May be fixed in the future
        """
        return capture(
            self, execute=execute, save_results=save_results, save_stdin=save_stdin
        )

    def __repr__(self):
        return f"RcloneCLI(remote={self.remote!r})"

    def __truediv__(self, new):
        return RcloneFile(self, new)


### RcloneCLI Objects that can be used for source
class RcloneFile:
    """
    Object representing many rclone operations. Note that move and moveto will
    update in place. Copy/Copyto does not
    """

    def __init__(self, rcloneobj, remoteitem=""):
        self.rclone = rcloneobj
        self.remoteitem = remoteitem
        self.path = RcloneCLI.pathjoin(self.rclone.remote, self.remoteitem)
        self.fs_remote = self.rclone.remote, self.remoteitem

    def __truediv__(self, new):
        # Need to decide if using RcloneCLI.pathjoin or os.path.join
        if self.remoteitem:
            return RcloneFile(self.rclone, os.path.join(self.remoteitem, new))
        return RcloneFile(self.rclone, new)

    def __repr__(self):
        return f"RcloneFile({self.path})"

    def __str__(self):
        return f"{self.path}"

    def _partial_remoteitem(self, name, *args, **kwargs):
        """Make self.remoteitem the first argument"""
        func = getattr(self.rclone, name)
        return func(self.remoteitem, *args, **kwargs)

    def _partial_remoteitemR(self, name, *args, **kwargs):
        """Rightsided"""
        func = getattr(self.rclone, name)
        return func(*args, self.remoteitem, **kwargs)

    # Right sided partial
    upload = partialmethod(_partial_remoteitemR, "uploadto")
    write = partialmethod(_partial_remoteitemR, "write")

    # Left sided partial
    download = partialmethod(_partial_remoteitem, "download")
    downloadto = partialmethod(_partial_remoteitem, "downloadto")
    read = partialmethod(_partial_remoteitem, "read")
    delete = partialmethod(_partial_remoteitem, "delete")

    copy = partialmethod(_partial_remoteitem, "copy")
    moveto = partialmethod(_partial_remoteitem, "moveto")
    copyto = partialmethod(_partial_remoteitem, "copyto")
    open = partialmethod(_partial_remoteitem, "open")
    info = iteminfo = partialmethod(_partial_remoteitem, "iteminfo")

    def move(self, remotedst, **kwargs):
        r = self.rclone.move(self.remoteitem, remotedst, **kwargs)
        self.remoteitem = os.path.join(remotedst, os.path.basename(self.remoteitem))
        return r

    def moveto(self, remotedst, **kwargs):
        r = self.rclone.moveto(self.remoteitem, remotedst, **kwargs)
        self.remoteitem = remotedst
        return r


### File Object
class _RawRcloneFileObj(io.RawIOBase):
    """Do not call this without a buffer wrapping it!"""

    def __init__(self, remotefile, rclone, *, flags=None, callopts=None):
        self.rclone = rclone
        self.remotefile = remotefile
        self.offset = 0
        self.flags = flags
        self.callopts = callopts
        self.maxsize = None

    def seekable(self):
        return True

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            self.offset = offset
        elif whence == io.SEEK_CUR:
            self.offset += offset
        else:  # whence = io.SEEK_END
            raise io.UnsupportedOperation()
        return self.offset

    def tell(self):
        return self.offset

    def readable(self):
        return True

    def writable(self):
        return False

    def readinto(self, b):
        if self.maxsize and self.offset >= self.maxsize:
            return 0
        N = len(b)
        chunk = self.rclone.read(
            self.remotefile,
            offset=self.offset,
            count=N,
            flags=self.flags,
            callopts=self.callopts,
        )
        n = len(chunk)
        self.offset += n
        if n != N:  # We know we hit the end
            self.maxsize = self.offset
        b[:n] = chunk
        return n


### Utilities


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
    from queue import Queue
    from threading import Thread

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


def _flagify(flags):
    flags = flags or []
    if isinstance(flags, str):
        flags = [flags]
    return list(flags)


def _dictify(mydict):
    if not mydict:
        return {}
    if isinstance(mydict, (list, tuple)):
        return dict(mydict)
    return mydict


class enable_check_dest:
    """
    Remove --no-check-dest or --ignore-times
    """

    def __init__(self, rclone):
        self.rclone = rclone

    def __enter__(self):
        self.af = self.rclone.always_flag
        self.rclone.always_flag = self.rclone.NOFLAG

    def __exit__(self, exc_type, exc_value, traceback):
        self.rclone.always_flag = self.af


class capture:
    """
    Capture rclone calls and optionally return False to no continue.

    WARNING: setting execute=False may cause errors downstream!

    Variables:
    ----------
    command_history
    command_results (iff save_results and execute)
    command_stdin (iff save_stdin and stdin is bytes or string)

    """

    def __init__(self, rclone, execute=True, save_results=False, save_stdin=False):
        self.rclone = rclone
        self.execute = execute
        self.save_results = save_results
        self.save_stdin = save_stdin
        self.command_history = []
        self.command_stdin = []
        self.command_results = []

    def __enter__(self):
        self._oldPopen, self.rclone._Popen = self.rclone._Popen, self._Popen
        self._oldcall, self.rclone.call = self.rclone.call, self._call
        self.rclone._capture = True
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.rclone._Popen = self._oldPopen
        self.rclone.call = self._oldcall
        self.rclone._capture = False

    def _Popen(self, cmd, *args, **kwargs):
        self.command_history.append(cmd)
        if self.execute:
            return subprocess.Popen(cmd, *args, **kwargs)
        return False

    def _call(self, *args, **kwargs):
        if self.save_stdin:
            stdin = kwargs.get("stdin", None)
            if isinstance(stdin, (str, bytes)):
                self.command_stdin.append(stdin)
            elif stdin:
                self.command_stdin.append(b"**not bytes**")
            else:
                self.command_stdin.append(None)
        else:
            self.command_stdin.append(None)

        res = self._oldcall(*args, **kwargs)
        if self.save_results:
            self.command_results.append(res)
        return res

    def shell_script(self, export=True, cd=True):
        """
        Return a string that represents the shell script to do the same as the results.

        Options:
        -------
        export [True]
            Include  `export` statements of the rclone environment

        cd [True]
            Include a `cd` command to the current directory
        """
        import shlex

        out = []
        if cd:
            cmd = ["cd", os.path.abspath(os.getcwd())]
            out.append(shlex.join(cmd))
        if export:
            for key, value in self.rclone.universal_env.items():
                if value in ("**UNSET**", self.rclone.DELENV):
                    out.append(f"unset {key}")
                    continue
                out.append(f"export {key}={shlex.quote(value)}")
        for stdin, cmd in zip(self.command_stdin, self.command_history):
            cmd = shlex.join(cmd)
            if stdin:
                if isinstance(stdin, bytes):
                    try:
                        stdin = stdin.decode("utf8")
                    except:
                        stdin = "**not UTF-8**"
                        out.append("# WARNING -- Could not understand stdin")
                if stdin == "**not bytes**":
                    out.append(
                        "# WARNING -- stdin specified and not bytes. May not work"
                    )
                cmd = shlex.join(["echo", stdin]) + f" | {cmd}"
            out.append(cmd)

        return "\n".join(out)


def check_returncode(proc, output=None, stderr=None):
    """Raise CalledProcessError if the exit code is non-zero."""

    if proc.returncode:
        raise subprocess.CalledProcessError(
            proc.returncode, proc.args, output=output, stderr=stderr
        )
