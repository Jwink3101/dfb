import argparse
import logging
import os
import sys
from functools import partial
from textwrap import dedent

from . import __git_version__, __version__

logger = logging.getLogger(__name__)

_TESTMODE = False

DFB_CONFIG = os.environ.get("DFB_CONFIG", os.environ.get("DFB_CONFIG_FILE", None))
argv = None

ISODATEHELP = """
    Specify a date and timestamp in an ISO-8601 like format (YYYY-MM-DD[T]HH:MM:SS) with
    or without spaces, colons, dashes, "T", etc. Can optionally
    specify a numeric time zone (e.g. -05:00) or 'Z'. If no timezone is specified, 
    it is assumed *local* time. Alternatively, can specify unix time with a preceding
    'u' (e.g. 'u1678560662'). Or can specify a time difference from the current
    time with any (and only) of the following: second[s], minute[s], hour[s], day[s], 
    week[s]. Example: "10 days 1 hour 4 minutes 32 seconds". (The order doesn't matter). 
    Can also specify "now" for the current time.
    """


# This lets me control how argparse exits.
# https://stackoverflow.com/a/14728477
class ThrowingArgumentParserError(Exception):
    pass


class ThrowingArgumentParser(argparse.ArgumentParser):
    """Like regular argument parser but throws an exception"""

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
        $DFB_CONFIG environment variable or is implied if executing the config
        file itself. $DFB_CONFIG is currently 
        {('set to ' + repr(DFB_CONFIG)) if DFB_CONFIG else 'not set'}. 
        """
    if shebanged:
        config_help += f" Currently implied as {shebanged!r}."
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
            "control overrides. See readme for details. "
            "Can specify multiple times. There is no input validation so do not specify "
            "untrusted inputs."
        ),
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

    when_parent = argparse.ArgumentParser(add_help=False)
    when_group = when_parent.add_argument_group(
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
    restore_parent = argparse.ArgumentParser(add_help=False, parents=[when_parent])
    restore_parent.add_argument(
        "--no-check",
        action="store_true",
        help="Disable rclone comparing the source and the dest. If set, will restore everything",
    )

    exe_shell_parent = argparse.ArgumentParser(add_help=False)
    exe_dump_parent = argparse.ArgumentParser(add_help=False)

    exe_groups = []
    for par in (exe_shell_parent, exe_dump_parent):
        pargroup = par.add_argument_group(
            title="Execution Settings",
            description="Precedance follows the order specified in this help",
        )
        # dry_int_group = dry_int_group0.add_mutually_exclusive_group()
        pargroup.add_argument(
            "-n", "--dry-run", action="store_true", help="Do not execute any changes"
        )
        pargroup.add_argument(
            "-i",
            "--interactive",
            action="store_true",
            help="Display planned actions and prompt to continue or stop",
        )

        exe_groups.append(pargroup)

    exe_groups[0].add_argument(  # onto exe_shell_parent
        "--shell-script",
        metavar="FILE or -",
        help="""
            Rather than call rclone from within '%(prog)s', instead generate
            a shell script at %(metavar)s that will perform the same actions.
            This is useful to verify behavior and modify as needed. Note that
            this may not be perfect but should be close. Can specify "-" to print script
            """,
    )

    exe_groups[1].add_argument(  # onto exe_dump_parent
        "--dump",
        metavar="FILE or -",
        help="""
            ADVANCED USAGE. Will dump the JSONL data that represents the backup or prune. 
            This can be used to manually do the action and then with 
            `dfb advanced dbimport` to apply. Note that this is a more advanced
            form of --dry-run. If FILE ends in '.gz' or '.xz', it will be
            compressed respectively. 
            Can specify "-" to print the dump to stdout.
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
        parents=[global_parent, config_global, exe_dump_parent],
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
    backup.add_argument(
        "--refresh",
        action="store_true",
        help="""
            Refresh the local cache with a real listing of the remote destination. 
            This can be much slower as it must list all versions of all files
            however, it is useful if something has changed at the remote outside of
            %(prog)s (e.g., manual pruning). When used, will use 'remote_compare' attribute
            instead of 'compare'. This is the same as running `refresh` command but
            will list simultaneously.""",
    )
    backup.add_argument(
        "--refresh-use-snapshots",
        action=argparse.BooleanOptionalAction,
        dest="use_snapshots",
        default=True,
        help="""
            Whether or not to also download snapshots from the destination and
            update metadata. Note that the snapshots are _secondary_. They are
            not needed but enable src-to-src comparisons immediately after refresh
            and are faster for resolving references. Default: %(default)s.
            """,
    )

    #################################################
    ## Backup
    #################################################

    refresh = subparsers["refresh"] = subpar.add_parser(
        "refresh",
        parents=[global_parent, config_global],
        help="""
            Refresh the local cache with a real listing of the remote destination
            Same as calling backup with `--refresh` but
            can be used outside of a backup
            """,
    )
    refresh.add_argument(
        "--use-snapshots",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="""
            Whether or not to also download snapshots from the destination and
            update metadata. Note that the snapshots are _secondary_. They are
            not needed but enable src-to-src comparisons immediately after refresh
            and are faster for resolving references. Default: %(default)s.
            """,
    )

    #################################################
    ## restore-dir
    #################################################
    _h = "Restore a (sub)directory to a specified location"
    restore_dir = subparsers["restore-dir"] = subpar.add_parser(
        "restore-dir",
        aliases=["restore"],  # Need to reset below
        parents=[global_parent, config_global, restore_parent, exe_shell_parent],
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
        parents=[global_parent, config_global, restore_parent, exe_shell_parent],
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
    list_parent = argparse.ArgumentParser(add_help=False, parents=[when_parent])

    list_parent.add_argument(
        "path", default="", nargs="?", help="Starting path. Defaults to the top"
    )

    # Setting for listings
    list_parent_settings = argparse.ArgumentParser(add_help=False)
    list_group = list_parent_settings.add_argument_group(title="Listing Settings")
    list_group.add_argument(
        "--header",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Print a header where applicable. Default: %(default)s",
    )
    list_group.add_argument(
        "--head",
        default=None,
        type=int,
        metavar="N",
        help="Include the first %(metavar)s lines plus --tail (if set).",
    )
    list_group.add_argument(
        "--tail",
        default=None,
        type=int,
        metavar="N",
        help="Include --head (if set) plus the last %(metavar)s lines.",
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
            Note, if applicable, all ModTimes are always local regardless""",
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
            List deleted files with '<filename> (DEL)'. 
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
            Specify twice for versions, total_size, size, ModTime, Timestamp, path.
            If a file is a reference, the size will be that of the referent with an "R"
            appended.
            """,
    )

    ls.add_argument(
        "--list",
        "--list-only",
        dest="list_only",
        choices=["files", "dirs", "both"],
        default=None,
        help="""
            Only list files or directories (or dirs). Default 'both' normally or 
            'files' for --recursive mode.
            """,
    )

    ls.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        help="""
            List all items recursively
            """,
    )

    ls.add_argument(
        "--real-path",
        "--rpath",
        action="count",
        default=0,
        dest="rpath",
        help="""
            Print the relevant (based on time settings) real path (rpath) of file. 
            Specify one to print the real-path of a reference file and twice to print
            the referent. Multiple specifications only matter for reference files.
            """,
    )

    snap = subparsers["lsnaps"] = subpar.add_parser(
        "snapshot",
        parents=[global_parent, list_parent, config_global],
        help="""
            Recursively list the files in line-delimited JSON at the 
            optionally specified time
            """,
    )

    de_group = snap.add_mutually_exclusive_group()
    de_group.add_argument(
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
    de_group.add_argument(
        "-e",
        "--export",
        action="store_true",
        help="Export mode. Includes _all_ entries, not just the final one",
    )
    snap.add_argument(
        "-O",
        "--output",
        help="""
        Specify an output file. Otherwise will print to stdout. If the file ends in .gz
        or .xz, will use the respective compression.""",
    )

    tree = subparsers["tree"] = subpar.add_parser(
        "tree",
        parents=[global_parent, list_parent, config_global],
        help="""
            Recursively list files in a tree
            """,
    )
    tree.add_argument(
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

    tree.add_argument(
        "--max-depth",
        type=int,
        metavar="N",
        default=-1,
        help="""
            Specify depth. The original path is 1. Default is none
            """,
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
        "--rpath",
        dest="real_path",
        action="count",
        default=0,
        help="Include *full* real path of the file. Specify twice to include the rclone path",
    )

    timestamps = subparsers["timestamps"] = subpar.add_parser(
        "timestamps",
        parents=[global_parent, config_global, list_parent_settings, when_parent],
        help="""
            List all timestamps in the backup or within specified range. 
            Note that this will include ones that were nominally pruned but 
            without all files""",
    )
    timestamps.add_argument(
        "path",
        default="",
        nargs="?",
        help="""
            Starting path. Defaults to the top. Specifying a path will also change the 
            stats to _only_ consider that path
            """,
    )
    # timestamps.add_argument('path')
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
        parents=[global_parent, config_global, exe_dump_parent],
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
        "-N",
        "--keep-versions",
        default=0,
        type=int,
        dest="N",
        help="""
            Specify number of versions to keep past the specified time. This can be used
            to prune versions only. For example, to keep only the last 10 versions, 
            do "prune now -N 10". Can also be combined with a date. For example, to keep 
            the last 4 versions older than 30 days, specify "prune '30 days' -N 4". 
            Can also specify negative numbers to shift forward in 
            time (advanced usage). 
            """,
        # Not keeping this in the docs but...
        # For example, to prune the oldest 5 versions, you can do
        # "prune u0 -N -6" (where you need to do one additional to account for the oldest).
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

    #################################################
    ## Summary
    #################################################
    summary = subparsers["summary"] = subpar.add_parser(
        "summary",
        parents=[
            global_parent,
            # list_parent,
            config_global,
            when_parent,
            # list_parent_settings,
        ],
        help="Summary of files",
    )

    summary.add_argument(
        "path", default="", nargs="?", help="Starting path. Defaults to the top"
    )

    #################################################
    ## Advanced Subparser
    #################################################
    adv = subparsers["adv"] = subpar.add_parser(
        "advanced",
        help="Advanced functions. Run `%(prog)s advanced -h` for help",
    )

    subparsers["adv_main"] = adv_subpar = adv.add_subparsers(
        dest="command",
        title="Commands",
        required=True,
        metavar="command",
        description="Run `%(prog)s <command> -h` for help",
    )

    #################################################
    ## Advanced-Import
    #################################################

    dbimport = subparsers["dbimport"] = adv_subpar.add_parser(
        "dbimport",
        parents=[global_parent, config_global],
        help="Import an exported list",
        description="""
            [ADVANCED] Import file(s) and append to database. 
            Will overwrite any existing data if applicable. Note: Does *not* upload
            the import file lists to the remote as in a backup.
            """,
    )

    dbimport.add_argument(
        "files",
        nargs="*",
        help="""File(s) to import. Can be any rclone path including local. 
                Will automatically decompress .gz or .xz files""",
    )
    # This lets the user specify positional or flag arguments
    dbimport.add_argument(
        "--files",
        nargs="*",
        metavar="file",
        action="append",
        default=[],
        dest="files2",  # will be merged in later
        help="""File(s) to import. Can be any rclone path including local. 
                Will automatically decompress .gz or .xz files""",
    )

    dbimport.add_argument(
        "--dirs",
        nargs="*",
        metavar="dir",
        action="append",
        default=[],
        help="""Directories of files import. Can be any rclone path including local. 
                Will automatically decompress .gz or .xz files. Will always import
                files then directories""",
    )

    dbimport.add_argument(
        "--reset",
        action="store_true",
        help="Reset the DB before import. Call without files to *just* reset",
    )
    dbimport.add_argument(
        "--upload",
        action="store_true",
        help="""
            Uploads the imported file(s). Will put them in a subdirectory of the snapshots
            with the current time and label each file as 'N.{filename}'
            """,
    )

    #################################################
    ## Advanced prune path
    #################################################
    prunepath = subparsers["prunepath"] = adv_subpar.add_parser(
        "prune-file",
        parents=[global_parent, config_global, exe_dump_parent],
        help="Prune a specific file (real-path or rpath)",
        description="""
            [ADVANCED] Prune a specific "real-path" (or "rpath") from the database,
            optionally including all references to the path.
            """,
    )

    prunepath.add_argument(
        "rpath",
        help="""
            Specify rpath(s) ("real paths") to prune. These are the paths where the 
            file is actually stored such as what you see with `versions --real-path`            
            """,
        nargs="+",
    )

    prunepath.add_argument(
        "--error-if-referenced",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="""
            If true (default), will error if there are references to the provided
            path(s). If false, will *also* delete those references.  Default: %(default)s.
        """,
    )
    #################################################
    ## Advanced timestamp-filters
    #################################################
    tsfilt = subparsers["tsfilt"] = adv_subpar.add_parser(
        "timestamp-include-filters",
        parents=[global_parent, config_global, when_parent],
        help="Create rclone --include filters for a time range",
        description="""
            Given a range of times specified, create and print rclone 
            --include filters that can be used for other rclone operations 
            (e.g. ls, ncdu) on the destination. Note that the filters are not perfect
            and could possibly include additional items if they happen to have the same
            timestamp in the name
            """,
    )

    tsfilt.add_argument(
        "path",
        default="",
        nargs="?",
        help="""
            Starting path. Defaults to the top.
            """,
    )

    #################################################
    ## cli utils Subparser
    #################################################
    cliutil = subparsers["utils"] = subpar.add_parser(
        "utils",
        help="CLI utility functions. Run `%(prog)s utils -h` for help",
    )

    subparsers["utils_main"] = cliutil_subpar = cliutil.add_subparsers(
        dest="command",
        title="Commands",
        required=True,
        metavar="command",
        description="Run `%(prog)s <command> -h` for help",
    )

    #################################################
    ## cli utils apath2rpath
    #################################################

    a2r_parser = subparsers["apath2rpath"] = cliutil_subpar.add_parser(
        "apath2rpath",
        parents=[],
        help="Convert apparent path (apath) and date to a real path (rpath)",
        description="""
            Convert apparent path (apath) and date to a real path (rpath)
            """,
    )

    a2r_parser.add_argument(
        "--date",
        metavar="TIMESTAMP",
        help=f"Specify timestamp for the filenames. Default is current time. {ISODATEHELP}",
    )
    a2r_parser.add_argument(
        "-0",
        "--print0",
        action="store_true",
        help="Seperate multiple items with a null byte instead of newline",
    )

    a2r_parser.add_argument(
        "files",
        nargs="+",
        help="""
            Specify one or more files. If '-' is specified, will read stdin 
            (and automatically handle newlines or null-bytes).
            """,
    )

    #################################################
    ## cli utils apath2rpath
    #################################################

    r2a_parser = subparsers["rpath2apath"] = cliutil_subpar.add_parser(
        "rpath2apath",
        parents=[],
        help="Convert real path (rpath) an apparent path (apath) and date",
        description="""
            Convert real path (rpath) an apparent path (apath) and date. Returns data in
            JSONLines format with an ISO8601 date
            """,
    )

    r2a_parser.add_argument(
        "--timestamp-local",
        action="store_true",
        help="""
            Return timestamps in local time instead of UTC""",
    )

    r2a_parser.add_argument(
        "files",
        nargs="+",
        help="""
            Specify one or more files. If '-' is specified, will read stdin 
            (and automatically handle newlines or null-bytes).
            """,
    )

    #################################################
    ## DONE
    #################################################
    args = parser.parse_args(argv)
    args._argv0 = argv

    if getattr(args, "only", None):
        args.before = args.after = args.only

    return args


def clishebang(argv=None):
    """
    This will add the config and assume backup if it isn't specified.
    Generally, this works by catching a failure on the argument parser then
    adding 'backup' command. That emits an incorrect error so we *try* to avoid
    that by adding 'backup' if nothing else is specified. For example"

        ./config.py # Will add backup right away
        ./config.py --flag # will have erorr message but then work.

    It isn't perfect but reduces the number of messages.

    """
    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        print("Must specify a config", file=sys.stderr)
        sys.exit(2)

    config = argv[0]
    argv = argv[1:] + ["--config", config]

    # If argv is only len-2, no command has been specified. Therefore, try to skip
    # the try/except below since it will emit cli help when not needed. Will still
    # happen if any backup flags are used. Not a big deal.
    if len(argv) == 2:
        argv.insert(0, "backup")

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
    if cliconfig.command in {
        "backup",
        "refresh",
        "restore-dir",
        "restore-file",
        "prune",
        "prune-file",
        "dbimport",
    }:
        verbosity += 1
    verbosity += getattr(cliconfig, "verbose", 0) - getattr(cliconfig, "quiet", 0)
    verbosity = max([0, verbosity])

    # handle cli utils first since they do not need a config file.
    if cliconfig.command == "apath2rpath":
        from .cliutils import cli_apath2rpath

        return cli_apath2rpath(cliconfig)
    elif cliconfig.command == "rpath2apath":
        from .cliutils import cli_rpath2apath

        return cli_rpath2apath(cliconfig)

    try:
        add_params = {}
        add_params["subdir"] = getattr(cliconfig, "subdir", "")

        # config also sets logging
        config = Config(
            cliconfig.config,
            tmpdir=getattr(cliconfig, "temp_dir", None),
            verbosity=verbosity,
            add_params=add_params,
        )
    except Exception as E:
        logger.error(f"parse: {E}")
        sys.exit(2)

    try:
        config.cliconfig = cliconfig
        argv = cliconfig._argv0
        logger.debug(f"{argv = }")

        if cliconfig.command == "init":
            config._write_template(force=cliconfig.force_overwrite)
            print(f"New config in {cliconfig.config!r}")

            return

        config.parse(override_txt="\n".join(cliconfig.override))
        logger.debug(f"{cliconfig = }")

        if cliconfig.command == "_config":
            global _TESTMODE
            _TESTMODE = True
            return config

        ###########################################
        ## Call out to the actual workers
        ###########################################
        # This will handle the refresh on it's own so it can be concurrent
        if cliconfig.command == "backup":
            from .backup import Backup

            config._set_auto()

            back = Backup(config)  # Two steps so the object is initialized even
            back.run()  # ... if it fails
            return back

        elif cliconfig.command == "refresh":
            from .dstdb import DFBDST

            config._set_auto()

            DFBDST(config).reset(use_snapshots=cliconfig.use_snapshots)
            return config
        elif cliconfig.command == "dbimport":
            from .dstdb import DFBDST

            # Update CLI args
            cliconfig.files.extend(g for group in cliconfig.files2 for g in group)
            cliconfig.dirs = [g for group in cliconfig.dirs for g in group]

            DFBDST(config).dbimport(
                cliconfig.files,
                cliconfig.dirs,
                reset=cliconfig.reset,
                upload=cliconfig.upload,
            )
            return config
        elif cliconfig.command == "snapshot":
            from .listing import snapshot

            snapshot(config)
            return config
        elif cliconfig.command == "tree":
            from .listing import tree

            tree(config)
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

        elif cliconfig.command in ("prune", "prune-file"):
            from .prune import Prune

            prune = Prune(config)
            if cliconfig.command == "prune":
                prune.bydate()
            else:
                prune.byrpaths()
            return prune
        elif cliconfig.command == "summary":
            from .listing import summary

            summary(config)

        elif cliconfig.command == "timestamp-include-filters":
            from .listing import timestamp_include_filters

            timestamp_include_filters(config)
            return config
        else:
            logger.error(f"Unrecognized command {cliconfig.command!r}")
            return config

    except Exception as E:
        logger.error("")
        logger.error(str(E))
        logger.error("")

        try:
            # Call fail_shell iff cliconfig.command == "backup"
            if cliconfig.command == "backup" and config.fail_shell:
                from .utils import shell_runner

                logger.info("Running 'fail_shell' commands. Note: May not get logged")
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
                logger.info(
                    "Will attempt to save logs and/or snapshots if configured. May fail"
                )
                back.upload_logs()
        except:
            logging.error("Saving logs and running fail_shell didn't work")

        if (
            config.verbosity > 1
            or os.environ.get("DFB_DEBUG_RAISE_EXCEPTION", "").lower() == "true"
        ):
            raise

        if not _TESTMODE:
            sys.exit(1)
    finally:
        try:
            config.rc.stop()
        except:
            pass
