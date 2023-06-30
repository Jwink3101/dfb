import os, sys
import argparse
from functools import partial

from . import log, debug, __version__, __git_version__

_r = repr

_TESTMODE = False

DFB_CONFIG = os.environ.get("DFB_CONFIG_FILE", None)
argv = None

ISODATEHELP = """
    Specify a date and timestamp in an ISO-8601 like format (YYYY-MM-DD[T]HH:MM:SS) with
    or without spaces, colons, dashes, "T", etc. Can optionally
    specify a numeric time zone (e.g. -05:00) or 'Z'. If no timezone is specified, 
    it is assumed *local* time. Alternatively, can specify unix time with a preceding
    'u'. Example: 'u1678560662'. Or can specify a time difference from the current
    time with any (and only) of the following: second[s], minute[s], hour[s], day[s], 
    week[s]. Example: "10 days 1 hour 4 minutes 32 seconds". (The order doesn't matter).
    """


# THis lets me control how argparse exits.
# https://stackoverflow.com/a/14728477
class ThrowingArgumentParserError(Exception):
    pass


class ThrowingArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_usage(sys.stderr)
        args = {"prog": self.prog, "message": message}
        msg = ("%(prog)s: error: %(message)s\n") % args
        raise ThrowingArgumentParserError(msg)


def parse(argv=None, shebanged=False):
    if argv is None:
        argv = sys.argv[1:]
    # Hacked commands
    if argv and argv[0] == "help":
        argv[0] = "--help"
    if argv and argv[0] == "version":
        argv[0] = "--version"

    # config_global_group = global_parent.add_argument_group(title="Config Settings")
    config_global = argparse.ArgumentParser(add_help=False)
    config_global_group = config_global.add_argument_group(
        title="Config & Cache Settings",
    )
    config_help = f"""
        (Required) Specify config file. Can also be specified via the 
        $DFB_CONFIG_FILE environment variable or is implied if executing the config
        file itself. $DFB_CONFIG_FILE is currently 
        {('set to ' + _r(DFB_CONFIG)) if DFB_CONFIG else 'not set'}. 
        """
    if shebanged:
        config_help += f" Currently implied as {_r(shebanged)}."
    config_global_group.add_argument(
        "--config",
        metavar="file",
        help=config_help,
        default=DFB_CONFIG,
        required=not bool(DFB_CONFIG),
    )
    config_global_group.add_argument(
        "-o",
        "--override",
        action="append",
        default=list(),
        metavar="'OPTION = VALUE'",
        help=(
            "Override any config option for this call only. Must be specified as "
            "'OPTION = VALUE', where VALUE should be proper Python (e.g. quoted strings). "
            """Example: --override "compare = 'mtime'". """
            "Override text is evaluated before *and* after the config file however, "
            "the variables 'pre' and 'post' are defined as True or False if it is "
            "before or after the config file. These can be used with conditionals to "
            "control overrides. See readme for details."
            "Can specify multiple times. There is no input validation of any sort."
        ),
    )

    config_global_group.add_argument(
        "--refresh",
        action="store_true",
        help="""
            Refresh the local cache with a real listing of the remote destination. 
            This can be much slower as it must list all versions of all files
            however, it is useful if something has changed at the remote outside of
            %(prog)s (e.g., manual pruning). When used, will use 'remote_compare' attribute
            instead of 'compare'.""",
    )

    global_parent = argparse.ArgumentParser(add_help=False)
    global_group = global_parent.add_argument_group(
        title="Global Settings",
        description="Default verbosity is 1 for backup/restore/prune and 0 for listing",
    )
    global_group.add_argument(
        "-v", "--verbose", "--debug", action="count", help="+1 verbosity", default=0
    )
    global_group.add_argument(
        "-q", "--quiet", action="count", help="-1 verbosity", default=0
    )
    global_group.add_argument(
        "--temp-dir", help="Specify a temp dir. Otherwise will use Python's default"
    )
    global_group.add_argument(
        "--_return-config", action="store_true", help=argparse.SUPPRESS
    )

    restore_parent = argparse.ArgumentParser(add_help=False)
    restore_parent.add_argument(
        "--at",
        "--before",
        dest="at",
        metavar="TIMESTAMP",
        help=f"""Timestamp for the file to restore. {ISODATEHELP}""",
    )
    restore_parent.add_argument(
        "--no-check",
        action="store_true",
        help="Disable rclone comparing the source and the dest. If set, will restore everything",
    )

    exe_parent = argparse.ArgumentParser(add_help=False)
    exe_group = exe_parent.add_argument_group(
        title="Execution Settings",
        description="Precedance follows the order specified in this help",
    )
    # exe_group = exe_group0.add_mutually_exclusive_group()
    exe_group.add_argument(
        "-n", "--dry-run", action="store_true", help="Do not execute any changes"
    )
    exe_group.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="Display planned actions and prompt to continue or stop",
    )
    exe_group.add_argument(
        "--shell-script",
        metavar="DEST or -",
        help="""
            Rather than call rclone from within '%(prog)s', instead generate
            a shell script at %(metavar)s that will perform the same actions.
            This is useful to verify behavior and modify as needed. Note that
            this may not be perfect but should be close. Can specify "-" to print script
            """,
    )

    #### Main

    parser = ThrowingArgumentParser(
        description="dfb -- Date File Backup",
        prog="dfb",
        parents=[global_parent],
    )

    version = "%(prog)s-" + __version__
    if __git_version__:
        version += f"|{__git_version__['version']}"
    parser.add_argument("--version", action="version", version=version)

    subparsers = {}
    subparsers["main"] = subpar = parser.add_subparsers(
        dest="command",
        title="Commands",
        required=True,
        metavar="command",
        description="Run `%(prog)s <command> -h` for help",
    )

    #################################################
    ## Init
    #################################################

    init = subparsers["init"] = subpar.add_parser(
        "init",
        parents=[global_parent],
        help="write a new config file.",
    )
    init.add_argument(
        "--force-overwrite",
        action="store_true",
        help="Force %(prog)s to overwrite an existing config file",
    )
    init.add_argument(
        "config",
        metavar="config-file",
        help="Specify a config file destination",
    )

    #################################################
    ## Backup
    #################################################

    backup = subparsers["backup"] = subpar.add_parser(
        "backup",
        parents=[global_parent, config_global, exe_parent],
        help="Run a backup",
    )
    backup.add_argument(
        "--subdir",
        default="",
        help="""
            Backup only a subset of the source. Move tracking (referencing) will not 
            work *outside* the subdir. It will look like a delete and then a later backup.
            WARNING: rclone will list the source with the subdir as the remote. 
            Filters that assume a path and/or are anchored to the root 
            (i.e. start with '/'), will NOT be applied correctly. 
            Use '--dry-run' or '--interactive' to verify! The variable 'subdir' is
            also defined in the config file which can be used with conditionals. 
            ⚠⚠⚠USE WITH CAUTION!⚠⚠⚠
            """,
    )

    #################################################
    ## restore-dir
    #################################################
    _h = "Restore a (sub)directory to a specified location"
    restore_dir = subparsers["restore-dir"] = subpar.add_parser(
        "restore-dir",
        aliases=["restore"],  # Need to reset below
        parents=[global_parent, config_global, restore_parent, exe_parent],
        help=_h,
        description=_h,
    )
    restore_dir.add_argument(
        "--source-dir",
        dest="source",
        default="",
        help="Source directory. Default is the root",
    )

    # These aren't in restore_parent because (a) I need to control the order and (b)
    # the text is slightly different
    restore_dir.add_argument(
        "dest",
        help="""
        Destination directory. Can be a local destination (e.g. '/path/to/restore' or '.'), 
        an arbitrary rclone remote (e.g. myremote:restore/path) or relative
        to the configured source by specifying it as "@src" (e.g. @src/restore/path)
        """,
    )
    #################################################
    ## restore-file
    #################################################

    restore_file = subparsers["restore-file"] = subpar.add_parser(
        "restore-file",
        parents=[global_parent, config_global, restore_parent, exe_parent],
        help="Restore a file to a specified location, file, or to stdout",
    )

    restore_file.add_argument(
        "source", help="File in the Backup. Optionally at the specified time"
    )

    restore_file.add_argument(
        "dest",
        help="""
        Destination directory or file (if --to). Can be a local destination 
        (e.g. '/path/to/restore' or '.'), 
        an arbitrary rclone remote (e.g. myremote:restore/path), relative
        to the configured source by specifying it as "@src" (e.g. @src/restore/path), or 
        specify as '-' to print to stdout.
        
        """,
    )

    restore_file.add_argument(
        "--to",
        action="store_true",
        help="""
        Assumes 'dest' is a file, not a directory. (i.e., uses 'rclone copyto' 
        instead of 'rclone copy')""",
    )
    #################################################
    ## File List
    #################################################
    list_parent = argparse.ArgumentParser(add_help=False)

    when_group = list_parent.add_argument_group(
        title="Time Specification", description=f"All TIMESTAMPs: {ISODATEHELP}"
    )
    when_group.add_argument(
        "--at",
        "--before",
        dest="before",
        metavar="TIMESTAMP",
        help=f"""
            Timestamp at which to show the files. If not specified, will be the latest.
            Note that if '--after' is set, this will not be the full snapshot in time.
            """,
    )
    when_group.add_argument(
        "--after",
        metavar="TIMESTAMP",
        help=f"""
            Only show files after the specified time. Note that this means the '--at' will
            not be the full snapshot.
            """,
    )

    when_group.add_argument(
        "--only",
        metavar="TIMESTAMP",
        help=f"""
            Only show files AT the specified time. 
            Shortcut for '--before TIMESTAMP --after TIMESTAMP' since both are inclusive.
            Useful if the exact timestamp is known such as from the 'timestamps' command.
            """,
    )

    list_parent.add_argument(
        "path", default="", nargs="?", help="Starting path. Defaults to the top"
    )

    list_parent_settings = argparse.ArgumentParser(add_help=False)
    list_group = list_parent_settings.add_argument_group(title="Listing Settings")
    list_group.add_argument(
        "--no-header",
        action="store_true",
        help="Disable headers where applicable",
    )
    list_group.add_argument(
        "--human",
        action="store_true",
        help="Use human readable sizes",
    )
    list_group.add_argument(
        "--timestamp-local",
        action="store_true",
        help="""
            Specify timestamps in local time instead of UTC/Z (default). 
            Note, if applicable, all ModTimes are local""",
    )

    ls = subparsers["ls"] = subpar.add_parser(
        "ls",
        parents=[global_parent, list_parent, config_global, list_parent_settings],
        help="Cleanly list files and directories at the optionally specified time",
    )

    ls.add_argument(
        "-d",
        "--deleted",
        "--del",
        dest="deleted",
        action="count",
        default=0,
        help="""
            List deleted files too with '<filename> (DEL)'. 
            Specify twice to ONLY include deleted files""",
    )
    ls.add_argument(
        "--full-path", action="store_true", help="Show full path when listing subdirs"
    )
    ls.add_argument(
        "-l",
        "--long",
        action="count",
        default=0,
        help="""
            Long listing with size, ModTime, path. 
            Specify twice for versions, size, ModTime, Timestamp, path.
            """,
    )

    snap = subparsers["lsnaps"] = subpar.add_parser(
        "snapshot",
        parents=[global_parent, list_parent, config_global],
        help="Recursivly list the files in line-delineated JSON at the optionally specified time",
    )

    snap.add_argument(
        "-d",
        "--deleted",
        "--del",
        dest="deleted",
        action="count",
        default=0,
        help="""
            List deleted files as well.  
            Specify twice to ONLY include deleted files""",
    )
    snap.add_argument(
        "--output",
        help="""
        Specify an output file. Otherwise will print to stdout""",
    )

    versions = subparsers["versions"] = subpar.add_parser(
        "versions",
        epilog="""Fields are [reference_count],size,mtime,timestamp,[real-path]. 
            mtime is local and snapshot will depend on setting. 
            Size will be "D" for deleted items and *end* in "R" for a reference file""",
        parents=[global_parent, config_global, list_parent_settings],
        help="Show all versions of a file.",
    )
    versions.add_argument("filepath", help="Path to the file of interest")

    versions.add_argument(
        "--ref-count", action="store_true", help="Include the reference count"
    )
    versions.add_argument(
        "--real-path",
        action="count",
        default=0,
        help="Include *full* real path of the file. Specify twice to include the rclone path",
    )

    timestamps = subparsers["timestamps"] = subpar.add_parser(
        "timestamps",
        parents=[global_parent, config_global, list_parent_settings],
        help="""
            List all timestamps in the backup. Note that this will include
            ones that were nominally pruned but without all files""",
    )
    #################################################
    ## Pruning
    #################################################
    prune = subparsers["prune"] = subpar.add_parser(
        "prune",
        epilog="""
            Pruning takes into account reference files and delete markers that need to
            be kept. Note that after pruning, it may appear possible to restore older
            than the prune time but the results are very unlikely to be correct! It is
            due to files that are not-yet-modified or are referenced. The prune 
            algorithm may miss some delete makers that technically could be deleted but
            it is more efficient not to try to identify them.
        """,
        parents=[global_parent, config_global, exe_parent],
        help="Prune older versions of the files",
    )
    prune.add_argument(
        "when",
        help=f"""
        Specify file modification prune time. The modification time of a file is when
        the *next* file was written and not the original timestamp. 
        {ISODATEHELP.strip()}""",
    )
    prune.add_argument(
        "--subdir",
        default="",
        metavar="dir",
        help="""
            Prune only files in '%(metavar)s'. In order to ensure that references do not
            break, this is mostly just a filter of what will be pruned rather than a
            major performance enhancement.
            """,
    )

    args = parser.parse_args(argv)
    args._argv0 = argv

    if getattr(args, "only", None):
        args.before = args.after = args.only

    return args


def clishebang(argv=None):
    """This will add the config and assume backup if it isn't specified'"""
    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        print("Must specify a config", file=sys.stderr)
        sys.exit(2)

    config = argv[0]
    argv = argv[1:] + ["--config", config]
    try:
        cliconfig = parse(argv, shebanged=config)
    except ThrowingArgumentParserError as E:
        try:
            cliconfig = parse(["backup"] + argv)
        except ThrowingArgumentParserError as E:
            print(*E.args, file=sys.stderr)
            sys.exit(2)
    r = _cli(cliconfig)
    if _TESTMODE:
        return r


def cli(argv=None):
    try:
        cliconfig = parse(argv)
    except ThrowingArgumentParserError as E:
        print(*E.args, file=sys.stderr)
        sys.exit(2)
    r = _cli(cliconfig)
    if _TESTMODE:
        return r


def _cli(cliconfig):
    # Reset aliases. I wish this was how it always worked.
    if cliconfig.command == "restore":
        cliconfig.command = "restore-dir"

    from .configuration import Config

    verbosity = 0
    if cliconfig.command in {"backup", "restore-dir", "restore-file", "prune"}:
        verbosity += 1
    verbosity += getattr(cliconfig, "verbose", 0)
    verbosity -= getattr(cliconfig, "quiet", 0)
    verbosity = max([0, verbosity])

    try:
        add_params = {}
        add_params["subdir"] = getattr(cliconfig, "subdir", "")

        config = Config(
            cliconfig.config,
            tmpdir=getattr(cliconfig, "temp_dir", None),
            verbosity=verbosity,
            add_params=add_params,
        )
    except Exception as E:
        print("ERROR: " + str(E), file=sys.stderr)
        sys.exit(2)

    try:
        config.cliconfig = cliconfig
        argv = cliconfig._argv0
        debug(f"{argv = }")

        if cliconfig.command == "init":
            config._write_template(force=cliconfig.force_overwrite)
            log.print(f"New config in {_r(cliconfig.config)}")

            return

        config.parse(override_txt="\n".join(cliconfig.override))
        debug(f"{cliconfig = }")

        if getattr(cliconfig, "_return_config", False):
            global _TESTMODE
            _TESTMODE = True
            return config

        ###########################################
        ## Call out to the actual workers
        ###########################################
        # This will handle the refresh on it's own so it can be concurrent
        if cliconfig.command == "backup":
            from .backup import Backup

            back = Backup(config)  # Two steps so the object is initialized even
            back.run()  # ... if it fails
            return back

        # Handle refresh on the others
        if getattr(cliconfig, "refresh", False):
            from .dstdb import DFBDST

            DFBDST(config).reset()

        ################

        if cliconfig.command == "snapshot":
            from .listing import snapshot

            snapshot(config)
            return config
        elif cliconfig.command == "ls":
            from .listing import ls

            ls(config)
            return config
        elif cliconfig.command == "versions":
            from .listing import file_versions

            file_versions(config)
            return config
        elif cliconfig.command == "timestamps":
            from .listing import timestamps

            timestamps(config)

        elif cliconfig.command in ("restore-dir", "restore-file"):
            from .restore import Restore

            return Restore(config)

        elif cliconfig.command == "prune":
            from .prune import Prune

            return Prune(config)

    except Exception as E:
        log("ERROR: " + str(E), file=sys.stderr, verbosity=0)
        log(
            f"ERROR Occured. See logs (including debug) at {_r(str(config.tmpdir.resolve()))}",
            file=sys.stderr,
            verbosity=0,
        )
        try:
            # Call fail_shell iff cliconfig.command == "backup"
            if cliconfig.command == "backup" and config.fail_shell:
                from .utils import shell_runner

                log("Running 'fail_shell' commands. Note: May not get logged")
                env = {
                    "LOGPATH": str(log.log_file.resolve()),
                    "DEBUGPATH": str(log.debug_file.resolve()),
                    "CONFIGDIR": os.path.dirname(config._configpath),
                }
                shell_runner(
                    config.fail_shell,
                    dry=config.cliconfig.dry_run,
                    env=env,
                    prefix="fail",
                )
            if cliconfig.command == "backup":
                log(
                    "Will attempt to save logs and/or snapshots if configured. May fail"
                )
                back.upload_logs()
        except:
            print("Saving logs and running fail_shell didn't work", file=sys.stderr)

        if config.verbosity > 1:
            raise

        if not _TESTMODE:
            sys.exit(1)
    finally:
        try:
            config.rc.stop_rc()
        except:
            pass
