import sys, os
import time
import tempfile
import uuid
import copy
import logging
import io
import hashlib, base64
from functools import partial, partialmethod
from pathlib import Path
from threading import Lock

logger = logging.getLogger(__name__)


LOCK = Lock()
_TEMPDIR = False  # Just used in testing

INF = float("inf")


class NotDFBFilter(logging.Filter):
    def __init__(self, include_serve=False):
        self.include_serve = include_serve
        super().__init__()

    def filter(self, record):
        if record.name.endswith("-rc-server") and not self.include_serve:
            return False
        return record.name.startswith("dfb.")


def init_logging(logfile, debuglogfile, verbosity):
    """
    Start logging. If _TEMPDIR is set, create a second one that always saves with
    more detail
    """
    USE_DEBUGFILE = bool(_TEMPDIR)

    levels = [logging.WARN, logging.INFO, logging.DEBUG]

    verbosity = min([len(levels), max([0, verbosity])])  # +1 for rc server
    if verbosity == len(levels):
        not_dfb_filter = NotDFBFilter(include_serve=True)
        verbosity -= 1
    else:
        not_dfb_filter = NotDFBFilter()

    level = levels[verbosity]

    formatter = logging.Formatter(
        fmt="%(asctime)s:%(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",  # "%z" removed
    )

    # Set up handlers with the level since the root_logger *may* be set lower
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)
    file_handler.addFilter(not_dfb_filter)

    stream_handler = logging.StreamHandler(stream=sys.stderr)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(level)
    stream_handler.addFilter(not_dfb_filter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()  # Need to clear them  for testing

    root_logger.setLevel(level=0 if USE_DEBUGFILE else level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)

    if USE_DEBUGFILE:
        dformatter = logging.Formatter(
            fmt=(
                "%(asctime)s.%(msecs)d:"  # https://stackoverflow.com/a/7517430/3633154
                "%(name)s:"
                # "%(module)s:"
                "%(lineno)d:"
                "%(funcName)s:"
                "%(levelname)s: "
                "%(message)s"
            ),
            datefmt="%Y%m%d%H%M%S",
        )

        dfile_handler = logging.FileHandler(debuglogfile)
        dfile_handler.setFormatter(dformatter)
        dfile_handler.setLevel(logging.DEBUG)
        dfile_handler.addFilter(NotDFBFilter(include_serve=True))

        root_logger.addHandler(dfile_handler)


def clean_config_id(config_id):
    """Cleans it up to alpha-numeric only"""
    config_id0 = config_id
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-[]"
    config_id = "".join(c if c in allowed else "=" for c in config_id)

    if len(config_id) > 40:
        md5 = base64.urlsafe_b64encode(
            hashlib.md5(config_id.encode()).digest()
        ).decode()
        config_id = f"{config_id[:20]}.{md5[:8]}.{config_id[-20:]}"

    if config_id0 != config_id:
        logger.debug(f"config_id changed from {config_id0!r} to {config_id!r}")

    return config_id


class ConfigError(ValueError):
    pass


class Config:
    def __init__(self, configpath, tmpdir=None, verbosity=1, add_params=None):
        from . import nowfun, __version__, __git_version__

        self._config = {"_configpath": configpath, "verbosity": verbosity}
        try:
            self.configpath = Path(configpath).resolve()  # make it absolute
        except FileNotFoundError:
            raise FileNotFoundError(f"Couldn't find {configpath!r}")
        self.add_params = add_params or {}
        self.add_params["subdir"] = self.add_params.get("subdir", "")

        self.now = nowfun()

        if _TEMPDIR:  # Testing
            self.tmpdir = Path(_TEMPDIR)
        elif not tmpdir:
            self.tmpdir = Path(tempfile.TemporaryDirectory().name)
        else:
            self.tmpdir = Path(tmpdir) / f"{int(self.now.dt)}"

        self.tmpdir.mkdir(parents=True, exist_ok=True)

        # Start the logging
        self.logfile = self.tmpdir / "log.log"
        self.debuglogfile = self.tmpdir / "debug.log"
        init_logging(self.logfile, self.debuglogfile, verbosity)

        logger.info(f"DFB ({__version__})")
        if __git_version__:
            logger.info(f" {__git_version__['version']} {__git_version__['origin']}")
        logger.info(f"Now: {self.now.obj.astimezone().isoformat()}")
        logger.info(f"Backup Timestamp: {self.now.dt}Z")
        logger.info(f"config path: '{self.configpath}'")
        logger.info(f"tmpdir: {str(self.tmpdir)}")

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

        logger.debug(f"Wrote template config to {self.configpath}")

    def parse(self, override_txt=""):
        from .rclonerc import RC

        if self.configpath is None:
            raise ValueError("Must have a config path")

        # Passed to the config file
        self._config["stats"] = 30  # Fixed. Undocumented that this can be set
        self._config["os"] = os
        self._config["Path"] = Path
        self._config["log"] = lambda x: logger.info(f"config: {x}")
        self._config["print"] = lambda x: logger.info(f"config: {x}")
        self._config["__file__"] = self.configpath
        self._config["__dir__"] = self.configpath.parent
        self._config["DELENV"] = RC.DELENV
        self._config["clean_config_id"] = clean_config_id

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

        logger.info(f"ID: {self.config_id}")
        logger.debug(f"Read config {str(self.configpath)!r}")
        for k in self._config_keys:
            if k not in self._config:
                continue
            dispval = self._config[k]
            if k == "rclone_env":
                dispval = {
                    n: (k if n != "RCLONE_CONFIG_PASS" else "**REDACTED**")
                    for n, k in dispval.items()
                }
            logger.debug(f"   {k} = {dispval!r}")

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
            # self.rclone_flags.append("--metadata")

        # Set up rclone objects. For the most part, we use the RC interface
        # but there are a few things where the CLI is better or easier. These may
        # go away in future versions. The CLI interface (rclonecli.py) has many
        # "features" that are eclipsed by rc, but a few are better on CLI.
        #
        # Add these here since we will need them throughout the eval. Note that
        # rc is using the rclone server while (src/dst)_rclone is making CLI
        # calls to rclone.
        from .rclonecli import RcloneCLI
        from .rclonerc import RC

        self.rc = RC(
            rclone_exe=self.rclone_exe,
            serve_flags=self.rclone_flags
            + ["-vv"],  # always include verbose but filter later
            rclone_env=self.rclone_env,
        )

        self.src_rclone = RcloneCLI(self._config["src"], **settings)
        self.dst_rclone = RcloneCLI(self._config["dst"], **settings)
        # Monkey patch the debug
        self.src_rclone.debug = lambda x: logger.debug(f"src-rclone: {x}")
        self.dst_rclone.debug = lambda x: logger.debug(f"dst-rclone: {x}")

        # Set the db_cachedir here. Needed to wait for rclone cli objects
        if not (dbcache_dir := self._config.get("dbcache_dir", None)):
            dbcache_dir = Path(self.dst_rclone.config_paths["Cache dir"]) / "DFB"
        self._config["dbcache_dir"] = Path(dbcache_dir)
        self._config["snap_cache_dir"] = self.dbcache_dir / f"{self.config_id}.snap"

        return self

    def _validate(self):
        """
        Validate config
        """
        from .rclonerc import FILTER_FLAGS

        if self.src == "<<MUST SPECIFY>>":
            raise ConfigError("Must specify 'src'")
        if self.dst == "<<MUST SPECIFY>>":
            raise ConfigError("Must specify 'dst'")

        allowed = {
            "compare": {"mtime", "size", "hash", "auto"},
            "dst_compare": {"mtime", "size", "hash", "auto", None},
            "renames": {"size", "mtime", "hash", "auto", False, None},
            "dst_renames": {"size", "mtime", "hash", "auto", False, None},
            "rename_method": {"reference", "copy", False, None},
            "get_modtime": {True, False, "auto"},
            "get_hashes": {True, False, "auto"},
        }

        for key, values in allowed.items():
            val = self._config[key]
            if val not in values:
                msg = f"Allowed values for '{key}' are {values}. Specified {repr(val)}"
                raise ConfigError(msg)
        ff = FILTER_FLAGS.union(["--one-file-system"])
        if badflags := ff.intersection(self.rclone_flags):
            msg = f"May not have {badflags} in 'rclone_flags'. Use 'filter_flags'"
            raise ConfigError(msg)

        # These will also set to auto if the compare or renames is auto
        self._config["dst_compare"] = (
            self._config["dst_compare"] or self._config["compare"]
        )
        if self._config["dst_renames"] is None:  # explicit because could be False
            self._config["dst_renames"] = self._config["renames"]

        if not self._config.get("dst_atomic_transfer", True):
            logger.info(
                "WARNING: 'dst_atomic_transfer = False' is deprecated since rclone 1.63 "
                "handles it for non-atomic remotes"
            )

        if self._config.get("links", False):
            logger.info(
                "WARNING: 'links' is deprecated. The link setting should be specified in rclone_flags"
            )

        # Set the config_id but give preference to _uuid
        self._config["config_id"] = clean_config_id(
            self._config.get("_uuid", self._config["config_id"])
        )

        if mrs := self._config["min_rename_size"]:
            from .utils import parse_bytes

            self._config["min_rename_size"] = mrs1 = parse_bytes(mrs)
            logger.debug(f"Parsed min_rename_size {mrs!r} as {mrs1!r} bytes")

    def _set_auto(self):
        sf = self.rc.features(self.src)
        df = self.rc.features(self.dst)

        src_mtime = sf["Precision"] < 1.1e9 and not sf["Features"]["SlowModTime"]
        dst_mtime = df["Precision"] < 1.1e9 and not df["Features"]["SlowModTime"]

        if self.compare == "auto":  # src to src
            self.compare = "mtime" if src_mtime else "size"
            logger.debug(f"auto-setting 'compare' to {self.compare!r}")

        if self.dst_compare == "auto":  # src to dst
            # don't *just* do self.compare since it could be hash
            if self.compare != "size" and src_mtime and dst_mtime:
                self.dst_compare = "mtime"
            else:
                self.dst_compare = "size"

            logger.debug(f"auto-setting 'dst_compare' to {self.dst_compare!r}")

        if self.renames == "auto":  # src to src
            self.renames = "mtime" if src_mtime else False
            logger.debug(f"auto-setting 'renames' to {self.renames!r}")

        if self.dst_renames == "auto":  # src to dst
            # don't *just* do self.renames since it could be hash or False
            if self.renames != "size" and self.renames and src_mtime and dst_mtime:
                self.dst_renames = "mtime"
            else:
                self.dst_renames = False

            logger.debug(f"auto-setting 'dst_renames' to {self.dst_renames!r}")

        if self.get_modtime == "auto":
            self.get_modtime = src_mtime
            logger.debug(f"auto-setting 'get_modtime' to {self.get_modtime!r}")

        if self.get_hashes == "auto":
            # self.get_hashes = sf["Hashes"] and not sf["Features"]["SlowHash"]
            # if sf['String'].startswith('S3 '):
            #    logger.warning(
            #        "src is S3 remote. S3 doesn't *always* provide hashes without "
            #        "additional API calls. Setting get_hashes to False to be safe"
            #    )
            #    self.get_hashes = False
            # logger.debug(f"auto-setting 'get_hashes' to {self.get_hashes!r}")

            # to be safe, making this always false. Users should set this if they want it
            self.get_hashes = False
            logger.debug(f"setting 'get_hashes' to False regardless of remotes")

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
            f"{k}={cfg[k]!r}" for k in self._config_keys if k in self._config
        )
        return f"Config({contents})"


TEMPLATE = r'''#!/usr/bin/env dfbshebanged
"""
DFB Config File

This configuration file is read as Python so things can be customized as
desired. With few exceptions, any missing items will go to the defaults
already specified.

rclone flags should always be a list. 
Example: `--exclude myfile` will be ['--exclude','myfile']

Defaults are sensible for a local source. Change as needed for others.

A few modules, including `os` and `Path = pathlib.Path` are already loaded along
with `logger.info()` and `logger.debug()`

Also defines:
    __file__ : Absolute path of the config file. pathlib.Path
    __dir__ : Absolute path of the config file parent. pathlib.Path
    subdir : Value of '--subdir' if specified, otherwise an empty string.

ALL LOCAL PATHS SHOULD BE ABSOLUTE
"""
##############################################
##              Basic Settings              ##
##                (required)                ##
##############################################

# Specify the source and destination rclone remotes
src = "<<MUST SPECIFY>>"
dst = "<<MUST SPECIFY>>"

##############################################
##                 Filters                  ##
##############################################

# Specify flags used for filtering. See https://rclone.org/filtering/
#
# If using flags like '--filter-from', they should be absolute paths. Example:
#   ["--filter-from", __dir__ / "myfilters.txt"]
#
# Note that when backups use --subdir, the paths specified here may be incorrect to the
# source. The variable 'subdir' is defined to assist. USE WITH CAUTION
filter_flags = []


##############################################
## Comparison and Rename-Tracking Settings  ##
##           (defaults to "auto")           ##
##############################################

# src-to-src comparison to determine changes. Auto tries to use mtime if supported.
compare = "auto"  # "size", "mtime", "hash", "auto"

# src-to-dst comparison such as after a refresh without snapshots. None uses 'compare'
dst_compare = None  # "size", "mtime", "hash", "auto", None

# Rename tracking src-to-src. False disables it. "size" is risky!
renames = "auto"  # "size", "mtime", "hash", "auto", False

# Rename tracking src-to-dst. None uses 'renames'
dst_renames = None  # "size", "mtime", "hash", "auto", False, None

# Rename files with server-side-copy or with reference file
rename_method = "reference"  # 'reference', 'copy'

# If files are less than this size, do not track the rename. Can specify with
# normal prefixes or an integer byte count (e.g. 2097152, "2 KiB", "10 MB", "15 MiB")
min_rename_size = 0

##############################################
##             rclone Settings              ##
##         (optional, intermediate)         ##
##############################################

# Flags and environment for general rclone usage.
# Examples include --config (or RCLONE_CONFIG).
# Example for config password
#   > from getpass import getpass
#   > rclone_env = {"RCLONE_CONFIG_PASS": getpass("Password: ")}
#
# Notes:
# - This IS where you should specify link flags for local such as 
#   --links, --skip-links, --copy-links
# - Not all flags are compatible
rclone_flags = []
rclone_env = {}

# Flags for refresh specifically. Example: --fast-list
dst_list_rclone_flags = []

# Executable
rclone_exe = "rclone"

##############################################
##          Intermediate Settings           ##
##                (optional)                ##
##############################################

# Number of transfers. Use remote-specific flags for additional control such as
# --s3-upload-concurrency.
concurrency = os.cpu_count()

# Tolerance on mtimes
dt = 1.0  # seconds

# Whether to always get src mtime. Can be expensive and slow on some remotes.
get_modtime = "auto"  # True, False, "auto"

# Allow missing hash. Falls back to "size"
error_on_missing_hash = False

# String or list of strings of hash types. None uses all available on a remote
hash_type = None

# Whether to always request hashes. "auto" maps to False for now
get_hashes = False  # True, False, "auto"

# Request and transfer (if possible) metadata. Metadata is also stored in the snapshot
# files in case the dst doesn't support it. However, it must be restored manually if not
# supported. Changes in file metadata will *not* force a backup again.
metadata = True

# Specify additional log destinations. These should be full rclone remote directories
# to save the log in addition to the remote. String or list of strings
log_dest = None

# Unique name for this config file. Default is just based on the src and dst. Will be
# processed and cleaned as needed
config_id = f"{src}-{dst}"

# Specify where to store the file database. Default (None) is
# `<rclone cache dir>/DFB/<config_id>.db`.
dbcache_dir = None

# If True, dfb will add an empty directory marker file, ".dfbempty", for empty 
# directories. These markers will be present in a restore but can later be deleted
empty_directory_markers = False

##############################################
##             Disable Features             ##
##     Use --override to undo. Example:     ##
##    --override "disable_prune = False"    ##
##############################################

# Pruning is the only distructive process in dfb. In order to make sure it isn't done by
# accident, setting this to True will force all pruneing to be as if called with
disable_prune = False

# While not destructive, refresh can be very slow on some remotes, especially with many
# references and/or not using snapshots
disable_refresh = False

##############################################
##       Pre- and Post-Shell Options        ##
##                (optional)                ##
##############################################

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
'''
