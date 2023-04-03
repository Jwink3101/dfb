import sys, os
import time
from pathlib import Path
import tempfile
import uuid
import copy
from functools import partial, partialmethod
import io
from threading import Lock

LOCK = Lock()
_TEMPDIR = False  # Just used in testing


class Log:
    def __init__(self):
        self.verbosity = 1

    def _init(self, *, tmpdir, verbosity):
        self.tmpdir = tmpdir = Path(tmpdir)
        tmpdir.mkdir(parents=True, exist_ok=True)

        self.log_file = self.tmpdir / "log.log"  # msg <= verbosity
        self.debug_file = self.tmpdir / "debug.log"  # msg > verbosity
        self.verbosity = verbosity
        debug(f"log started. {tmpdir = } {verbosity = }")

    def log(self, *args, prefix="", verbosity=1, print_mode=False, **kwargs):
        """print() to the log with date"""
        verbosity = int(verbosity)

        t = [time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())]
        if verbosity == 2:
            t.append("DEBUG")
        elif verbosity > 2:
            t.append(f"DEBUG({verbosity})")

        if prefix:
            if isinstance(prefix, list):
                prefix = ".".join(p for p in prefix if p)
            t.append(prefix)

        t = ".".join(t) + ": "
        if print_mode:
            t = ""

        with io.StringIO() as sio:
            kwargs["file"] = sio
            kwargs["end"] = ""

            print(*args, **kwargs)

            lines = sio.getvalue().split("\n")
            lines = [t + line for line in lines]

        lines = "\n".join(lines)

        del kwargs["file"]

        with LOCK:  # Append should be atomic but just in case, lock it along with print
            if verbosity > self.verbosity:
                # I may choose to comment this out in the future. I like the idea of
                # always being able to access the more verbose log but the compares can
                # get to be too much...
                with open(self.debug_file, mode="at") as fobj:
                    print(lines, file=fobj)
            else:
                with open(self.log_file, mode="at") as fobj:
                    print(lines, file=fobj)
                print(lines)

    __call__ = log
    debug = partialmethod(log, verbosity=2)
    ddebug = partialmethod(log, verbosity=3)
    print = partialmethod(log, verbosity=0, print_mode=True)


log = Log()  # Still needs to be _init. Done in the config
debug = log.debug
ddebug = log.ddebug


class ConfigError(ValueError):
    pass


class Config:
    def __init__(self, configpath, tmpdir=None, verbosity=1, add_params=None):
        from . import nowfun, __version__, __git_version__

        self._config = {"_configpath": configpath, "verbosity": verbosity}
        try:
            self.configpath = Path(configpath).resolve()  # make it absolute
        except FileNotFoundError:
            raise FileNotFoundError(f"Couldn't find {repr(configpath)}")
        self.add_params = add_params or {}

        self.now = nowfun()

        if _TEMPDIR:  # Testing
            self.tmpdir = Path(_TEMPDIR)
        elif not tmpdir:
            self.tmpdir = Path(tempfile.TemporaryDirectory().name)
        else:
            self.tmpdir = Path(tmpdir) / f"{int(self.now.dt)}"

        self.tmpdir.mkdir(parents=True, exist_ok=True)

        # Start the logging
        log._init(tmpdir=self.tmpdir, verbosity=verbosity)
        log(f"DFB ({__version__})")
        if __git_version__:
            log(f" {__git_version__['version']} {__git_version__['origin']}")
        log(f"Now: {self.now.obj.astimezone().isoformat()}")
        log(f"Backup Timestamp: {self.now.dt}Z")
        log(f"config path: '{self.configpath}'")
        log(f"tmpdir: {str(self.tmpdir)}")

    def _write_template(self, force=False):
        from . import __version__

        txt = TEMPLATE
        txt = txt.replace("__VERSION__", __version__)
        txt = txt.replace("__UUID4__", str(uuid.uuid4()))

        if self.configpath.exists() and not force:
            raise ValueError(
                f"Path '{self.configpath}' exists. "
                "Specify a different path, move the existing file "
                "or use --force-overwrite"
            )
        self.configpath.parent.mkdir(parents=True, exist_ok=True)
        self.configpath.write_text(txt)

        os.chmod(self.configpath, self.configpath.stat().st_mode | 0o100)

        debug(f"Wrote template config to {self.configpath}")

    def parse(self, override_txt=""):
        from .rclone import RC

        if self.configpath is None:
            raise ValueError("Must have a config path")

        # Passed to the config file
        self._config["os"] = os
        self._config["Path"] = Path
        self._config["log"] = self._config["print"] = partial(log, prefix="config")
        self._config["log0"] = log
        self._config["debug"] = partial(debug, prefix="config")
        self._config["__file__"] = self.configpath
        self._config["__dir__"] = self.configpath.parent
        self._config["DELENV"] = RC.DELENV

        self._config.update(self.add_params)

        self._hidden_keys = set(self._config)  # to be removed in repr
        self._hidden_keys.difference_update({"configpath", "verbosity"})

        junk = {}
        exec("", junk)

        exec(TEMPLATE, self._config)  # Defaults
        self._config_keys = [
            k
            for k in self._config
            if (k not in junk and k not in self._hidden_keys and not k.startswith("_"))
        ]
        self._config_keys.append("destpaths")

        # Read then change dir
        config_txt = self.configpath.read_text()
        # os.chdir(self._config["__dir__"])

        # Set the override_txt before AND after so that you can set other things
        cfg = [
            "pre,post = True,False",
            override_txt,
            config_txt,
            "pre,post = False,True",
            override_txt,
        ]
        exec("\n".join(cfg), self._config)

        for key in junk:
            self._config.pop(key, 0)

        self._validate()

        debug(f"Read config {self.configpath}")
        for k in self._config_keys:
            if k not in self._config:
                continue
            dispval = self._config[k]
            if k == "rclone_env":
                dispval = {
                    n: (k if n != "RCLONE_CONFIG_PASS" else "**REDACTED**")
                    for n, k in dispval.items()
                }
            debug(f"   {k} = {dispval}")

        # Set these up because all methods will use them
        settings = dict(
            rclone_exe=self.rclone_exe,
            universal_flags=self.rclone_flags,
            universal_env=self.rclone_env,
            # Even if using something like Google Drive which allows duplicates,
            # we won't *ever* be overwriting files so this is okay.
            no_check_dest=True,
        )
        if self.metadata:
            settings["universal_flags"].append("--metadata")

        # Add these here since we will need them
        from .rcloneapi import Rclone as RcloneAPI
        from .rclone import RC

        self.rc = RC(
            rclone_exe=self.rclone_exe,
            serve_flags=self.rclone_flags + ["-vv"],
            rclone_env=self.rclone_env,
            serve_log_callback=partial(debug, prefix="rc"),
        )
        self.rc.kill_at_exit()

        self.src_rclone = RcloneAPI(self._config["src"], **settings)
        self.dst_rclone = RcloneAPI(self._config["dst"], **settings)
        # Monkey patch the debug
        self.src_rclone.debug = partial(debug, prefix="src-rclone")
        self.dst_rclone.debug = partial(debug, prefix="dst-rclone")

        return self

    def _validate(self):
        """
        Validate config
        """
        from .rclone import FILTER_FLAGS

        if self.src == "<<MUST SPECIFY>>":
            raise ConfigError("Must specify 'src'")
        if self.dst == "<<MUST SPECIFY>>":
            raise ConfigError("Must specify 'dst'")

        allowed = {
            "compare": {"mtime", "size", "hash"},
            "dst_compare": {"mtime", "size", "hash", None},
            "renames": {"size", "mtime", "hash", False, None},
            "reuse_hashes": {"size", "mtime", False, None},
            "links": {"skip", "link", "copy"},
        }

        for key, values in allowed.items():
            val = self._config[key]
            if val not in values:
                raise ConfigError(
                    f"Allowed values for '{key}' are {values}. Specified '{val}'"
                )

        badflags = FILTER_FLAGS.intersection(self.rclone_flags)
        if badflags:
            raise ConfigError(
                f"May not have {badflags} in 'rclone_flags'. Use 'filter_flags'"
            )

        self._config["dst_compare"] = (
            self._config["dst_compare"] or self._config["compare"]
        )
        if self._config["dst_renames"] is None:  # explicit because could be False
            self._config["dst_renames"] = self._config["renames"]

        if self._config["links"] == "copy":
            self._config["rclone_flags"].append("--copy-links")

    def __getattr__(self, attr):
        return self._config[attr]

    def __setattr__(self, attr, value):
        if attr.startswith("_"):
            return super(Config, self).__setattr__(attr, value)

        self._config[attr] = value

    def __repr__(self):
        # Need to watch out for RCLONE_CONFIG_PASS in rclone_env
        # make a copy of the dict fixing that one but do not
        # just do a deepcopy in case the user imported modules
        cfg = copy.copy(self._config)
        cfg["rclone_env"] = cfg["rclone_env"].copy()

        if "RCLONE_CONFIG_PASS" in cfg["rclone_env"]:
            cfg["rclone_env"]["RCLONE_CONFIG_PASS"] = "**REDACTED**"

        contents = ", ".join(
            f"{k}={repr(cfg[k])}" for k in self._config_keys if k in self._config
        )
        return f"Config({contents})"


TEMPLATE = r'''#!/usr/bin/env dfbshebanged
"""
DFB Config File

This configuration file is read as Python so things can be customized as
desired. With few exceptions, any missing items will go to the defaults
already specified.

Rclone flags should always be a list. 
Example: `--exclude myfile` will be ['--exclude','myfile']

Defaults are sensible for a local source. Change as needed for others.

A few modules, including `os` and `Path = pathlib.Path` are already loaded along
with `log()` and `debug()`

Also defines:
    __file__ : Absolute path of the config file. pathlib.Path
    __dir__ : Absolute path of the config file parent. pathlib.Path
    subdir : Value of '--subdir' if specified, otherwise an empty string.

ALL LOCAL PATHS SHOULD BE ABSOLUTE
"""
# Specify the source and destination. if local, no need to specify in
# rclone remote format BUT SHOULD BE ABSOLUTE
src = "<<MUST SPECIFY>>"
dst = "<<MUST SPECIFY>>"

# Specify FILTERING flags only. Note that if filtering flags are used later,
# it *will* cause issues. Examples of rclone filters:
#     --include --exclude --include-from --exclude-from --filter --filter-from
#     --exclude-if-present
#
# Must be specified as a list, e.g., ["--exclude","*.exc"]
# If using flags like ''--filter-from', they should be absolute. Could do:
#   ["--filter-from", __dir__ / "myfilters.txt"]
#
# Note that when backups use --subdir, the paths specified here may be incorrect to the
# source. The variable 'subdir' is defined to assist. USE WITH CAUTION
filter_flags = []

# General rclone flags are called every time rclone is called. This is how
# you can specify things like the conifg file.
# Remember that this config is evaluated from its parent directory.
#
# Example: ["--config", "path/to/config"]
#
# Note: Not all flags are compatible and may break the behavior, e.g. --progress
# Do NOT use flags like --links, --copy-links, or --skip-links here. See links below
rclone_flags = []

# The following are added to the existing environment.
# These should NOT include any filtering!
# Example: Getting config password
#   > from getpass import getpass
#   > rclone_env = {"RCLONE_CONFIG_PASS": getpass("Password: ")}
#
# This is also useful to set the cache dir which is used by DFB
#    > rclone_env = {'RCLONE_CACHE_DIR':'my/cache/dir'}
# Paths should be absolute.
rclone_env = {}

# Due to https://github.com/rclone/rclone/issues/6855 in rclone, dfb manually
# handles links. This isn't perfect and there may be minor edge cases around non-local
# remotes with files ending in .rclonelink.
# Options:
#   'skip' : Same as --skip-links (DEFAULT)_
#   'link' : Same as --links. Will create a .rclonelink file
#   'copy' : Same as --copy-links. Will copy the file itself
#
# If using a non-local remote, ignore this.
# Note that symlinks are not restored. The will maintain the rclonelink extension but this
# can be easily fixed after restore.
links = 'skip' # {'skip','link','copy'}

# This sets the number of individual file transfers at a time and the general
# concurrency of rclone calls. Note that there are other rclone flags that will
# split transfers into more connections as well.
concurrency = os.cpu_count()

# Some remotes such as local, FTP, and SFTP do not have atomic uploads. They can have
# incomplete uploads took as if they are finished. If (src/dst)_atomic_transfer is False,
# it will upload to a temp name and then move it.
# Note that for restores, you must use a flag to do non-atomic transfers.
dst_atomic_transfer = True 

# Specify the attributes to decide if a source file is modified.
#   "size"  : Did the size change. Acceptable but easy to have false negative
#   "mtime" : Did the size and modification time change. Requires that source has
#             ModTime. Can even use --use-server-modtime flags on the source
#   "hash"  : Use the hash. Note that using 'hash' with `reuse_hashes = 'mtime'`
#           : is *effectivly* still mtime
compare = "mtime"  # "size", "mtime", "hash"

# Generally, comparisons are done from source-to-source but if the information
# comes from the destination such as after a '--refresh', different attributes
# may be used if the destination does not support the same attributes of the
# source (e.g. use mtime on a local source but destination is WebDAV which
# doesn't support it), you can specify an alternative compare attribute.
# Options are the same as for `compare` plus `None` which means to use the same.
dst_compare = None  # None means use `compare`

# When listing the destination directly from --refresh, you can specify additional
# flags that you may otherwise not need. For example, if the destination is S3, you
# may with to include --fast-list
dst_list_rclone_flags = []

# moves/renames can be tracked if the file is unmodified other than the name.
#
# Tracking is done via the following. Note that the pool of considered
# files are *only* those that have been identified as new.
#
#   'size'    : Size of the file only. Not very safe. Use with extreme caution
#   'mtime'   : mtime and size. Slightly safer than size but still risky
#   'hash'    : Hash of the files.
#    False    : Disable rename tracking
#
# Renamed files are references to the original.
renames = "mtime"

# Similarly, renames on a destination-based file information can be different
dst_renames = None # None means use 'renames'

# When doing mtime comparisons, what is the error to allow. Ideally, this
# should be small since it is always on the same machine but some filesystems
# have some slack.
dt = 1.1  # seconds

# Some remotes (e.g. S3) require an additional API call to get modtimes. If you
# are comparing with 'size' of 'hash', you can forgo this API call by setting
# this to False. Note that this may be ignored if modtimes are needed.
# Note that the destination modtimes are ONLY ever gotten if needed
get_modtime = True  # True means you DO save ModTime at the source

# Hashes can be expensive to compute on "SlowHash" remotes such as local or sftp.
# As such, rather than recompute them all, the hashes of the previous state
# can be used if they match based on this setting. If this is set, unmatched files
# are hased in a second call.
#
#   "size"  : Reuse hashes if filename and size match the previous
#   "mtime" : Reuse hashes if the filename, size, and mtime (within dt) match
#   False   : Do NOT reuse hashes. Note: Setting this to False on a "SlowHash"
#             remote *and* requiring hashes through other settings will be very slow.
reuse_hashes = False # "mtime"

# Some remotes (notably local) allow for multiple hash types. If this is specified
# AND hashes need to be computed, you can set the types. Specify as a single item
# (e.g. hash_type = 'sha1') or as a tuple (e.g. hash_type = 'sha1','md5'). If None
# (default), will do all possible
hash_type = None

# Even if the hashes are not needed for compare or move-tracking, it can be helpful
# to have the file hashes. It is NOT recommended for "SlowHash" remotes (e.g. local,
# sftp) unless you ALSO have `reuse_hashes` set. Note thatthis may be ignored if
# hashes are needed anyway.
get_hashes = False

# Specify the path to the rclone executable.
rclone_exe = "rclone"

# How often to report stats. Especially useful when listing slower remotes.
stats = 30 # seconds

# Specify whether to upload logs to the remote. They will be in .dfb/logs
upload_logs = True

# Specify additional log directories. These should be full rclone remote directories
# to save the log in addition to the remote. Can specify as a single string or a
# list/tuple/etc if you wish to use multiple.
log_dest = None
# log_dest = "/full/path/to/local"
# log_dest = "/full/path/to/local", "remote:path/to/log"

# Also store and transfer metadata. Uses rclone's metadata capabilities
# as outlined at https://rclone.org/docs/#metadata.
# Not all remotes (either source or dest) can read/write this but, if it can
# be read on the source, it will be preserved in the filelist
metadata = True

## Pre-, Post- and fail- shell commands to run
# Specify shell code to be evaluated before and/or after running dfb. Note
# these are all run from the current directory but $CONFIGDIR environment will be set.
# STDOUT and STDERR will be captured. Note that there is no validation or
# security of the inputs. These are not actually called if using dry-run.
#
# The post shell call also has the "$STATS" environment variable defined which has
# the run statistics including timing (which will be different than the final since
# the logs will not yet have been dumped)
#
# Can be specified as the following:
#
#     string : Run with shell=True Can cd as needed (including `cd $CONFIGDIR`) and 
#              can be multiple lines and multiple commands.
#     list   : Will execute with shell=False in the current directory
#     dict   : Specify subprocess.Popen flags plus the keyword 'cmd'. YOU decide if
#              shell should be True or False based on 'cmd'. dfb will override settings
#              for std(out/err). Will update os.environ with any 'env' settings.
#
# If the final cmd if specified directly as a list or a list inside the dict, each
# command will be run through C-style formatting of the environ. C-Style is used to not 
# break the more-common str.format (or f-string) formats that may exist.
# os.environ may be updated with `env` inside of a dict.
#
# Example:
#   post_shell = 'echo "$STATS"'
#   post_shell = ["echo","%(STATS)s"]
# will look the same
pre_shell = ""
post_shell = ""

# Specify whether or not to allow an error in the shell commands above to continue
stop_on_shell_error = False

# If, and only if, the run fails, this will get called. It will not be logged or
# have any information about the failure however the temporary log, $LOGPATH, and
# debug log, $DEBUGPATH, could be read if interested. Useful for sending alert of
# failed run. Same rules as above about being specified as a list
fail_shell = ""


#######
# This should only be changed by the user when migrating from an older config
# to a newer one. Just because the current version of dbf and the version
# below do not match, it does not mean the run won't work.
_version = "__VERSION__"

# This is a random string that should be different in every config.
# It is used to define the name of the databases used in the sync. If it is changed, 
# the next run should be done with `--refresh` to start a new database.
_uuid = "__UUID4__"
'''
