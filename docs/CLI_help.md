# CLI Help


version: `dfb-20260621.0b0`  


# No Command


```text
usage: dfb [-h] [-v] [-q] [--temp-dir TEMP_DIR] [--version] command ...

dfb -- Date File Backup

options:
  -h, --help            show this help message and exit
  --version             show program's version number and exit

Global Settings:
  Default verbosity is 1 for backup/restore/prune and 0 for listing

  -v, --verbose, --debug
                        +1 verbosity
  -q, --quiet           -1 verbosity
  --temp-dir TEMP_DIR   Specify a temp dir. Otherwise will use Python's default

Commands:
  Run `dfb <command> -h` for help

  command
    init                write a new config file.
    mount               Mount a dfb view over an rclone mount (optional dependency).
    backup              Run a backup
    refresh             Refresh the local cache with a real listing of the remote
                        destination Same as calling backup with `--refresh` but can be
                        used outside of a backup
    restore-dir (restore)
                        Restore a (sub)directory to a specified location
    restore-file        Restore a file to a specified location, file, or to stdout
    ls                  Cleanly list files and directories at the optionally specified
                        time
    snapshot            Recursively list the files in line-delimited JSON at the
                        optionally specified time
    tree                Recursively list files in a tree
    versions            Show all versions of a file.
    timestamps          List all timestamps in the backup or within specified range.
                        Note that this will include ones that were nominally pruned
                        but without all files
    prune               Prune older versions of the files
    summary             Summary of files
    advanced            Advanced functions. Run `dfb advanced -h` for help
    utils               CLI utility functions. Run `dfb utils -h` for help

```

# init


```text
usage: dfb init [-h] [-v] [-q] [--temp-dir TEMP_DIR] [--force-overwrite] config-file

positional arguments:
  config-file           Specify a config file destination

options:
  -h, --help            show this help message and exit
  --force-overwrite     Force dfb init to overwrite an existing config file

Global Settings:
  Default verbosity is 1 for backup/restore/prune and 0 for listing

  -v, --verbose, --debug
                        +1 verbosity
  -q, --quiet           -1 verbosity
  --temp-dir TEMP_DIR   Specify a temp dir. Otherwise will use Python's default

```

# mount


```text
usage: dfb mount [-h] [--before TS] [--after AFTER]
                 [--remove-empty-dirs | --no-remove-empty-dirs]
                 [--empty-check-depth N] [--empty-check-fail {hide,show}]
                 [--undated-files {show,hide,tag}] [--cache | --no-cache]
                 [--cache-reset minutes] [--fuse-library path] [--show-deleted] [-q |
                 -v] [--version]
                 rclone_mount_loc mount_dest

dfb FUSE thin-wrapper mount. Must FIRST mount the destination with rclone. This
implementation relies on mfusepy (external) rather than a bundled FUSE shim.

positional arguments:
  rclone_mount_loc      Path where rclone is mounted
  mount_dest            Mount dest

options:
  -h, --help            show this help message and exit
  --before, --at TS     Specify a timestamp. Specify a date and timestamp in an
                        ISO-8601 like format (YYYY-MM-DD[T]HH:MM:SS) with or without
                        spaces, colons, dashes, "T", etc. Can optionally specify a
                        numeric time zone (e.g. -05:00) or 'Z'. If no timezone is
                        specified, it is assumed *local* time. Alternatively, can
                        specify unix time with a preceding 'u' (e.g. 'u1678560662').
                        Or can specify a time difference from the current time with
                        any (and only) of the following: second[s], minute[s],
                        hour[s], day[s], week[s]. Example: "10 days 1 hour 4 minutes
                        32 seconds". (The order doesn't matter). Can also specify
                        "now" for the current time.
  --after AFTER         Only show files after the specified time. Specify a date and
                        timestamp in an ISO-8601 like format (YYYY-MM-DD[T]HH:MM:SS)
                        with or without spaces, colons, dashes, "T", etc. Can
                        optionally specify a numeric time zone (e.g. -05:00) or 'Z'.
                        If no timezone is specified, it is assumed *local* time.
                        Alternatively, can specify unix time with a preceding 'u'
                        (e.g. 'u1678560662'). Or can specify a time difference from
                        the current time with any (and only) of the following:
                        second[s], minute[s], hour[s], day[s], week[s]. Example: "10
                        days 1 hour 4 minutes 32 seconds". (The order doesn't matter).
                        Can also specify "now" for the current time.
  --undated-files {show,hide,tag}
                        How to handle files without a datetag. 'show' keeps them as
                        normal files, 'hide' excludes them, and 'tag' shows them with
                        a trailing [NODATE]. Empty directory checks use this same
                        visibility behavior. Default: tag.
  --cache, --no-cache   Cache directory listings. Defaults to enabled when
                        --before/--at is used, otherwise disabled. For long-running
                        mounts, consider --no-cache so new backups are visible.
  --cache-reset minutes
                        Specify an interval, in minutes, to reset the cache. If not
                        set, cache will not reset
  --fuse-library path   Optional libfuse path/name (e.g. fuse-t) for macOS FUSE-T
                        setups.
  --show-deleted        Show deleted files with a trailing [DEL] suffix. Opening these
                        files returns literal [DEL] content. Note that directories can
                        still appear empty if you are viewing an earlier --before/--at
                        time and no files existed yet.
  -q, --quiet           Only show warnings and errors.
  -v, --verbose         Enable debug logging.
  --version             show program's version number and exit

Empty Directory Handling:
  --remove-empty-dirs, --no-remove-empty-dirs
                        Try to determine if a directory is empty and hide those with
                        no visible files. Due to dfb keeping all copies of files,
                        directories can remain after deletes. This may walk
                        subdirectories and can be expensive. Default: disabled (--no-
                        remove-empty-dirs).
  --empty-check-depth N
                        When --remove-empty-dirs is enabled, limit how deep to scan
                        for visible files. Depth 0 checks only the directory itself,
                        depth 1 checks one level below, and so on. Lower depths are
                        faster but less certain. Ignored unless --remove-empty-dirs is
                        set.
  --empty-check-fail {hide,show}
                        Behavior when --empty-check-depth cannot determine emptiness
                        because the depth limit is reached before a visible file is
                        found. 'hide' treats it as empty and hides the directory;
                        'show' keeps it visible. Default: hide.

Note on Mounting
----------------
This is a thin wrapper around a simple loop-back file system pointed to the dfb
destination. Rclone's mounting capabilities are more powerful, so this overlays
an rclone mount and presents the dfb view.

Example: Assume `dst = "backups:server"`

    $ rclone mount backups:server rclone_mount --vfs-cache-mode full --read-only
    $ dfb mount rclone_mount dfb_mount

You can mount a top-level remote and point dfb-mount at any subdirectory.

```

# backup


```text
usage: dfb backup [-h] [-v] [-q] [--temp-dir TEMP_DIR] --config file
                  [-o 'OPTION = VALUE'] [-n] [-i] [--dump FILE or -] [--subdir SUBDIR]
                  [--refresh] [--refresh-use-snapshots | --no-refresh-use-snapshots]
                  [--refresh-references | --no-refresh-references]

options:
  -h, --help            show this help message and exit
  --subdir SUBDIR       Backup only a subset of the source. Move tracking
                        (referencing) will not work *outside* the subdir. It will look
                        like a delete and then a later backup. WARNING: rclone will
                        list the source with the subdir as the remote. Filters that
                        assume a path and/or are anchored to the root (i.e. start with
                        '/'), will NOT be applied correctly. Use '--dry-run' or '--
                        interactive' to verify! The variable 'subdir' is also defined
                        in the config file which can be used with conditionals. ⚠⚠⚠USE
                        WITH CAUTION!⚠⚠⚠
  --refresh             Refresh the local cache with a real listing of the remote
                        destination. This can be much slower as it must list all
                        versions of all files however, it is useful if something has
                        changed at the remote outside of dfb backup (e.g., manual
                        pruning). When used, will use 'remote_compare' attribute
                        instead of 'compare'. This is the same as running `refresh`
                        command but will list simultaneously.
  --refresh-use-snapshots, --no-refresh-use-snapshots
                        Whether or not to also download snapshots from the destination
                        and update metadata. Note that the snapshots are _secondary_.
                        They are not needed but enable src-to-src comparisons
                        immediately after refresh and are faster for resolving
                        references. Default: True.
  --refresh-references, --no-refresh-references
                        Whether or not to resolve reference files after refresh
                        listing. Disabling skips reference resolution and keeps
                        unresolved reference markers in the cache. Default: True.

Global Settings:
  Default verbosity is 1 for backup/restore/prune and 0 for listing

  -v, --verbose, --debug
                        +1 verbosity
  -q, --quiet           -1 verbosity
  --temp-dir TEMP_DIR   Specify a temp dir. Otherwise will use Python's default

Config & Cache Settings:
  --config file         (Required) Specify config file. Can also be specified via the
                        $DFB_CONFIG environment variable or is implied if executing
                        the config file itself. $DFB_CONFIG is currently not set.
  -o, --override 'OPTION = VALUE'
                        Override any config option for this call only. Must be
                        specified as 'OPTION = VALUE', where VALUE should be proper
                        Python (e.g. quoted strings). Example: --override "compare =
                        'mtime'". Override text is evaluated before *and* after the
                        config file however, the variables 'pre' and 'post' are
                        defined as True or False if it is before or after the config
                        file. These can be used with conditionals to control
                        overrides. See readme for details. Can specify multiple times.
                        There is no input validation so do not specify untrusted
                        inputs.

Execution Settings:
  Precedance follows the order specified in this help

  -n, --dry-run         Do not execute any changes
  -i, --interactive     Display planned actions and prompt to continue or stop
  --dump FILE or -      ADVANCED USAGE. Will dump the JSONL data that represents the
                        backup or prune. This can be used to manually do the action
                        and then with `dfb advanced dbimport` to apply. Note that this
                        is a more advanced form of --dry-run. If FILE ends in '.gz' or
                        '.xz', it will be compressed respectively. Can specify "-" to
                        print the dump to stdout.

```

# refresh


```text
usage: dfb refresh [-h] [-v] [-q] [--temp-dir TEMP_DIR] --config file
                   [-o 'OPTION = VALUE'] [--use-snapshots | --no-use-snapshots]
                   [--refresh-references | --no-refresh-references]

options:
  -h, --help            show this help message and exit
  --use-snapshots, --no-use-snapshots
                        Whether or not to also download snapshots from the destination
                        and update metadata. Note that the snapshots are _secondary_.
                        They are not needed but enable src-to-src comparisons
                        immediately after refresh and are faster for resolving
                        references. Default: True.
  --refresh-references, --no-refresh-references
                        Whether or not to resolve reference files after refresh
                        listing. Disabling skips reference resolution and keeps
                        unresolved reference markers in the cache. Default: True.

Global Settings:
  Default verbosity is 1 for backup/restore/prune and 0 for listing

  -v, --verbose, --debug
                        +1 verbosity
  -q, --quiet           -1 verbosity
  --temp-dir TEMP_DIR   Specify a temp dir. Otherwise will use Python's default

Config & Cache Settings:
  --config file         (Required) Specify config file. Can also be specified via the
                        $DFB_CONFIG environment variable or is implied if executing
                        the config file itself. $DFB_CONFIG is currently not set.
  -o, --override 'OPTION = VALUE'
                        Override any config option for this call only. Must be
                        specified as 'OPTION = VALUE', where VALUE should be proper
                        Python (e.g. quoted strings). Example: --override "compare =
                        'mtime'". Override text is evaluated before *and* after the
                        config file however, the variables 'pre' and 'post' are
                        defined as True or False if it is before or after the config
                        file. These can be used with conditionals to control
                        overrides. See readme for details. Can specify multiple times.
                        There is no input validation so do not specify untrusted
                        inputs.

```

# restore-dir


```text
usage: dfb restore-dir [-h] [-v] [-q] [--temp-dir TEMP_DIR] --config file
                       [-o 'OPTION = VALUE'] [--at TIMESTAMP] [--after TIMESTAMP]
                       [--only TIMESTAMP] [--no-check] [-n] [-i] [--source-dir SOURCE]
                       dest

Restore a (sub)directory to a specified location

positional arguments:
  dest                  Destination directory. Can be a local destination (e.g.
                        '/path/to/restore' or '.'), an arbitrary rclone remote (e.g.
                        myremote:restore/path) or relative to the configured source by
                        specifying it as "@src" (e.g. @src/restore/path)

options:
  -h, --help            show this help message and exit
  --no-check            Disable rclone comparing the source and the dest. If set, will
                        restore everything
  --source-dir SOURCE   Source directory. Default is the root

Global Settings:
  Default verbosity is 1 for backup/restore/prune and 0 for listing

  -v, --verbose, --debug
                        +1 verbosity
  -q, --quiet           -1 verbosity
  --temp-dir TEMP_DIR   Specify a temp dir. Otherwise will use Python's default

Config & Cache Settings:
  --config file         (Required) Specify config file. Can also be specified via the
                        $DFB_CONFIG environment variable or is implied if executing
                        the config file itself. $DFB_CONFIG is currently not set.
  -o, --override 'OPTION = VALUE'
                        Override any config option for this call only. Must be
                        specified as 'OPTION = VALUE', where VALUE should be proper
                        Python (e.g. quoted strings). Example: --override "compare =
                        'mtime'". Override text is evaluated before *and* after the
                        config file however, the variables 'pre' and 'post' are
                        defined as True or False if it is before or after the config
                        file. These can be used with conditionals to control
                        overrides. See readme for details. Can specify multiple times.
                        There is no input validation so do not specify untrusted
                        inputs.

Time Specification:
  All TIMESTAMPs: Specify a date and timestamp in an ISO-8601 like format (YYYY-MM-
  DD[T]HH:MM:SS) with or without spaces, colons, dashes, "T", etc. Can optionally
  specify a numeric time zone (e.g. -05:00) or 'Z'. If no timezone is specified, it
  is assumed *local* time. Alternatively, can specify unix time with a preceding 'u'
  (e.g. 'u1678560662'). Or can specify a time difference from the current time with
  any (and only) of the following: second[s], minute[s], hour[s], day[s], week[s].
  Example: "10 days 1 hour 4 minutes 32 seconds". (The order doesn't matter). Can
  also specify "now" for the current time.

  --at, --before TIMESTAMP
                        Timestamp at which to show the files. If not specified, will
                        be the latest. Note that if '--after' is set, this will not be
                        the full snapshot in time.
  --after TIMESTAMP     Only show files after the specified time. Note that this means
                        the '--at' will not be the full snapshot.
  --only TIMESTAMP      Only show files AT the specified time. Shortcut for '--before
                        TIMESTAMP --after TIMESTAMP' since both are inclusive. Useful
                        if the exact timestamp is known such as from the 'timestamps'
                        command.

Execution Settings:
  Precedance follows the order specified in this help

  -n, --dry-run         Do not execute any changes
  -i, --interactive     Display planned actions and prompt to continue or stop

```

# restore-file


```text
usage: dfb restore-file [-h] [-v] [-q] [--temp-dir TEMP_DIR] --config file
                        [-o 'OPTION = VALUE'] [--at TIMESTAMP] [--after TIMESTAMP]
                        [--only TIMESTAMP] [--no-check] [-n] [-i] [--to]
                        source dest

positional arguments:
  source                File in the Backup. Optionally at the specified time
  dest                  Destination directory or file (if --to). Can be a local
                        destination (e.g. '/path/to/restore' or '.'), an arbitrary
                        rclone remote (e.g. myremote:restore/path), relative to the
                        configured source by specifying it as "@src" (e.g.
                        @src/restore/path), or specify as '-' to print to stdout.

options:
  -h, --help            show this help message and exit
  --no-check            Disable rclone comparing the source and the dest. If set, will
                        restore everything
  --to                  Assumes 'dest' is a file, not a directory. (i.e., uses 'rclone
                        copyto' instead of 'rclone copy')

Global Settings:
  Default verbosity is 1 for backup/restore/prune and 0 for listing

  -v, --verbose, --debug
                        +1 verbosity
  -q, --quiet           -1 verbosity
  --temp-dir TEMP_DIR   Specify a temp dir. Otherwise will use Python's default

Config & Cache Settings:
  --config file         (Required) Specify config file. Can also be specified via the
                        $DFB_CONFIG environment variable or is implied if executing
                        the config file itself. $DFB_CONFIG is currently not set.
  -o, --override 'OPTION = VALUE'
                        Override any config option for this call only. Must be
                        specified as 'OPTION = VALUE', where VALUE should be proper
                        Python (e.g. quoted strings). Example: --override "compare =
                        'mtime'". Override text is evaluated before *and* after the
                        config file however, the variables 'pre' and 'post' are
                        defined as True or False if it is before or after the config
                        file. These can be used with conditionals to control
                        overrides. See readme for details. Can specify multiple times.
                        There is no input validation so do not specify untrusted
                        inputs.

Time Specification:
  All TIMESTAMPs: Specify a date and timestamp in an ISO-8601 like format (YYYY-MM-
  DD[T]HH:MM:SS) with or without spaces, colons, dashes, "T", etc. Can optionally
  specify a numeric time zone (e.g. -05:00) or 'Z'. If no timezone is specified, it
  is assumed *local* time. Alternatively, can specify unix time with a preceding 'u'
  (e.g. 'u1678560662'). Or can specify a time difference from the current time with
  any (and only) of the following: second[s], minute[s], hour[s], day[s], week[s].
  Example: "10 days 1 hour 4 minutes 32 seconds". (The order doesn't matter). Can
  also specify "now" for the current time.

  --at, --before TIMESTAMP
                        Timestamp at which to show the files. If not specified, will
                        be the latest. Note that if '--after' is set, this will not be
                        the full snapshot in time.
  --after TIMESTAMP     Only show files after the specified time. Note that this means
                        the '--at' will not be the full snapshot.
  --only TIMESTAMP      Only show files AT the specified time. Shortcut for '--before
                        TIMESTAMP --after TIMESTAMP' since both are inclusive. Useful
                        if the exact timestamp is known such as from the 'timestamps'
                        command.

Execution Settings:
  Precedance follows the order specified in this help

  -n, --dry-run         Do not execute any changes
  -i, --interactive     Display planned actions and prompt to continue or stop

```

# ls


```text
usage: dfb ls [-h] [-v] [-q] [--temp-dir TEMP_DIR] [--at TIMESTAMP]
              [--after TIMESTAMP] [--only TIMESTAMP] --config file
              [-o 'OPTION = VALUE'] [--header | --no-header] [--head N] [--tail N]
              [--human] [--timestamp-local] [-d] [--full-path] [-l]
              [--list {files,dirs,both}] [-r] [--real-path]
              [path]

positional arguments:
  path                  Starting path. Defaults to the top

options:
  -h, --help            show this help message and exit
  -d, --deleted, --del  List deleted files with '<filename> (DEL)'. Specify twice to
                        ONLY include deleted files
  --full-path           Show full path when listing subdirs
  -l, --long            Long listing with size, ModTime, path. Specify twice for
                        versions, total_size, size, ModTime, Timestamp, path. If a
                        file is a reference, the size will be that of the referent
                        with an "R" appended.
  --list, --list-only {files,dirs,both}
                        Only list files or directories (or dirs). Default 'both'
                        normally or 'files' for --recursive mode.
  -r, --recursive       List all items recursively
  --real-path, --rpath  Print the relevant (based on time settings) real path (rpath)
                        of file. Specify one to print the real-path of a reference
                        file and twice to print the referent. Multiple specifications
                        only matter for reference files.

Global Settings:
  Default verbosity is 1 for backup/restore/prune and 0 for listing

  -v, --verbose, --debug
                        +1 verbosity
  -q, --quiet           -1 verbosity
  --temp-dir TEMP_DIR   Specify a temp dir. Otherwise will use Python's default

Time Specification:
  All TIMESTAMPs: Specify a date and timestamp in an ISO-8601 like format (YYYY-MM-
  DD[T]HH:MM:SS) with or without spaces, colons, dashes, "T", etc. Can optionally
  specify a numeric time zone (e.g. -05:00) or 'Z'. If no timezone is specified, it
  is assumed *local* time. Alternatively, can specify unix time with a preceding 'u'
  (e.g. 'u1678560662'). Or can specify a time difference from the current time with
  any (and only) of the following: second[s], minute[s], hour[s], day[s], week[s].
  Example: "10 days 1 hour 4 minutes 32 seconds". (The order doesn't matter). Can
  also specify "now" for the current time.

  --at, --before TIMESTAMP
                        Timestamp at which to show the files. If not specified, will
                        be the latest. Note that if '--after' is set, this will not be
                        the full snapshot in time.
  --after TIMESTAMP     Only show files after the specified time. Note that this means
                        the '--at' will not be the full snapshot.
  --only TIMESTAMP      Only show files AT the specified time. Shortcut for '--before
                        TIMESTAMP --after TIMESTAMP' since both are inclusive. Useful
                        if the exact timestamp is known such as from the 'timestamps'
                        command.

Config & Cache Settings:
  --config file         (Required) Specify config file. Can also be specified via the
                        $DFB_CONFIG environment variable or is implied if executing
                        the config file itself. $DFB_CONFIG is currently not set.
  -o, --override 'OPTION = VALUE'
                        Override any config option for this call only. Must be
                        specified as 'OPTION = VALUE', where VALUE should be proper
                        Python (e.g. quoted strings). Example: --override "compare =
                        'mtime'". Override text is evaluated before *and* after the
                        config file however, the variables 'pre' and 'post' are
                        defined as True or False if it is before or after the config
                        file. These can be used with conditionals to control
                        overrides. See readme for details. Can specify multiple times.
                        There is no input validation so do not specify untrusted
                        inputs.

Listing Settings:
  --header, --no-header
                        Print a header where applicable. Default: True
  --head N              Include the first N lines plus --tail (if set).
  --tail N              Include --head (if set) plus the last N lines.
  --human               Use human readable sizes
  --timestamp-local     Specify timestamps in local time instead of UTC/Z (default).
                        Note, if applicable, all ModTimes are always local regardless

```

# snapshot


```text
usage: dfb snapshot [-h] [-v] [-q] [--temp-dir TEMP_DIR] [--at TIMESTAMP]
                    [--after TIMESTAMP] [--only TIMESTAMP] --config file
                    [-o 'OPTION = VALUE'] [-d | -e] [-O OUTPUT]
                    [path]

positional arguments:
  path                  Starting path. Defaults to the top

options:
  -h, --help            show this help message and exit
  -d, --deleted, --del  List deleted files as well. Specify twice to ONLY include
                        deleted files
  -e, --export          Export mode. Includes _all_ entries, not just the final one
  -O, --output OUTPUT   Specify an output file. Otherwise will print to stdout. If the
                        file ends in .gz or .xz, will use the respective compression.

Global Settings:
  Default verbosity is 1 for backup/restore/prune and 0 for listing

  -v, --verbose, --debug
                        +1 verbosity
  -q, --quiet           -1 verbosity
  --temp-dir TEMP_DIR   Specify a temp dir. Otherwise will use Python's default

Time Specification:
  All TIMESTAMPs: Specify a date and timestamp in an ISO-8601 like format (YYYY-MM-
  DD[T]HH:MM:SS) with or without spaces, colons, dashes, "T", etc. Can optionally
  specify a numeric time zone (e.g. -05:00) or 'Z'. If no timezone is specified, it
  is assumed *local* time. Alternatively, can specify unix time with a preceding 'u'
  (e.g. 'u1678560662'). Or can specify a time difference from the current time with
  any (and only) of the following: second[s], minute[s], hour[s], day[s], week[s].
  Example: "10 days 1 hour 4 minutes 32 seconds". (The order doesn't matter). Can
  also specify "now" for the current time.

  --at, --before TIMESTAMP
                        Timestamp at which to show the files. If not specified, will
                        be the latest. Note that if '--after' is set, this will not be
                        the full snapshot in time.
  --after TIMESTAMP     Only show files after the specified time. Note that this means
                        the '--at' will not be the full snapshot.
  --only TIMESTAMP      Only show files AT the specified time. Shortcut for '--before
                        TIMESTAMP --after TIMESTAMP' since both are inclusive. Useful
                        if the exact timestamp is known such as from the 'timestamps'
                        command.

Config & Cache Settings:
  --config file         (Required) Specify config file. Can also be specified via the
                        $DFB_CONFIG environment variable or is implied if executing
                        the config file itself. $DFB_CONFIG is currently not set.
  -o, --override 'OPTION = VALUE'
                        Override any config option for this call only. Must be
                        specified as 'OPTION = VALUE', where VALUE should be proper
                        Python (e.g. quoted strings). Example: --override "compare =
                        'mtime'". Override text is evaluated before *and* after the
                        config file however, the variables 'pre' and 'post' are
                        defined as True or False if it is before or after the config
                        file. These can be used with conditionals to control
                        overrides. See readme for details. Can specify multiple times.
                        There is no input validation so do not specify untrusted
                        inputs.

```

# tree


```text
usage: dfb tree [-h] [-v] [-q] [--temp-dir TEMP_DIR] [--at TIMESTAMP]
                [--after TIMESTAMP] [--only TIMESTAMP] --config file
                [-o 'OPTION = VALUE'] [-d] [--max-depth N]
                [path]

positional arguments:
  path                  Starting path. Defaults to the top

options:
  -h, --help            show this help message and exit
  -d, --deleted, --del  List deleted files as well. Specify twice to ONLY include
                        deleted files
  --max-depth N         Specify depth. The original path is 1. Default is none

Global Settings:
  Default verbosity is 1 for backup/restore/prune and 0 for listing

  -v, --verbose, --debug
                        +1 verbosity
  -q, --quiet           -1 verbosity
  --temp-dir TEMP_DIR   Specify a temp dir. Otherwise will use Python's default

Time Specification:
  All TIMESTAMPs: Specify a date and timestamp in an ISO-8601 like format (YYYY-MM-
  DD[T]HH:MM:SS) with or without spaces, colons, dashes, "T", etc. Can optionally
  specify a numeric time zone (e.g. -05:00) or 'Z'. If no timezone is specified, it
  is assumed *local* time. Alternatively, can specify unix time with a preceding 'u'
  (e.g. 'u1678560662'). Or can specify a time difference from the current time with
  any (and only) of the following: second[s], minute[s], hour[s], day[s], week[s].
  Example: "10 days 1 hour 4 minutes 32 seconds". (The order doesn't matter). Can
  also specify "now" for the current time.

  --at, --before TIMESTAMP
                        Timestamp at which to show the files. If not specified, will
                        be the latest. Note that if '--after' is set, this will not be
                        the full snapshot in time.
  --after TIMESTAMP     Only show files after the specified time. Note that this means
                        the '--at' will not be the full snapshot.
  --only TIMESTAMP      Only show files AT the specified time. Shortcut for '--before
                        TIMESTAMP --after TIMESTAMP' since both are inclusive. Useful
                        if the exact timestamp is known such as from the 'timestamps'
                        command.

Config & Cache Settings:
  --config file         (Required) Specify config file. Can also be specified via the
                        $DFB_CONFIG environment variable or is implied if executing
                        the config file itself. $DFB_CONFIG is currently not set.
  -o, --override 'OPTION = VALUE'
                        Override any config option for this call only. Must be
                        specified as 'OPTION = VALUE', where VALUE should be proper
                        Python (e.g. quoted strings). Example: --override "compare =
                        'mtime'". Override text is evaluated before *and* after the
                        config file however, the variables 'pre' and 'post' are
                        defined as True or False if it is before or after the config
                        file. These can be used with conditionals to control
                        overrides. See readme for details. Can specify multiple times.
                        There is no input validation so do not specify untrusted
                        inputs.

```

# versions


```text
usage: dfb versions [-h] [-v] [-q] [--temp-dir TEMP_DIR] --config file
                    [-o 'OPTION = VALUE'] [--header | --no-header] [--head N]
                    [--tail N] [--human] [--timestamp-local] [--ref-count]
                    [--real-path]
                    filepath

positional arguments:
  filepath              Path to the file of interest

options:
  -h, --help            show this help message and exit
  --ref-count           Include the reference count
  --real-path, --rpath  Include *full* real path of the file. Specify twice to include
                        the rclone path

Global Settings:
  Default verbosity is 1 for backup/restore/prune and 0 for listing

  -v, --verbose, --debug
                        +1 verbosity
  -q, --quiet           -1 verbosity
  --temp-dir TEMP_DIR   Specify a temp dir. Otherwise will use Python's default

Config & Cache Settings:
  --config file         (Required) Specify config file. Can also be specified via the
                        $DFB_CONFIG environment variable or is implied if executing
                        the config file itself. $DFB_CONFIG is currently not set.
  -o, --override 'OPTION = VALUE'
                        Override any config option for this call only. Must be
                        specified as 'OPTION = VALUE', where VALUE should be proper
                        Python (e.g. quoted strings). Example: --override "compare =
                        'mtime'". Override text is evaluated before *and* after the
                        config file however, the variables 'pre' and 'post' are
                        defined as True or False if it is before or after the config
                        file. These can be used with conditionals to control
                        overrides. See readme for details. Can specify multiple times.
                        There is no input validation so do not specify untrusted
                        inputs.

Listing Settings:
  --header, --no-header
                        Print a header where applicable. Default: True
  --head N              Include the first N lines plus --tail (if set).
  --tail N              Include --head (if set) plus the last N lines.
  --human               Use human readable sizes
  --timestamp-local     Specify timestamps in local time instead of UTC/Z (default).
                        Note, if applicable, all ModTimes are always local regardless

Fields are [reference_count],size,mtime,timestamp,[real-path]. mtime is local and
snapshot will depend on setting. Size will be "D" for deleted items and *end* in "R"
for a reference file

```

# timestamps


```text
usage: dfb timestamps [-h] [-v] [-q] [--temp-dir TEMP_DIR] --config file
                      [-o 'OPTION = VALUE'] [--header | --no-header] [--head N]
                      [--tail N] [--human] [--timestamp-local] [--at TIMESTAMP]
                      [--after TIMESTAMP] [--only TIMESTAMP]
                      [path]

positional arguments:
  path                  Starting path. Defaults to the top. Specifying a path will
                        also change the stats to _only_ consider that path

options:
  -h, --help            show this help message and exit

Global Settings:
  Default verbosity is 1 for backup/restore/prune and 0 for listing

  -v, --verbose, --debug
                        +1 verbosity
  -q, --quiet           -1 verbosity
  --temp-dir TEMP_DIR   Specify a temp dir. Otherwise will use Python's default

Config & Cache Settings:
  --config file         (Required) Specify config file. Can also be specified via the
                        $DFB_CONFIG environment variable or is implied if executing
                        the config file itself. $DFB_CONFIG is currently not set.
  -o, --override 'OPTION = VALUE'
                        Override any config option for this call only. Must be
                        specified as 'OPTION = VALUE', where VALUE should be proper
                        Python (e.g. quoted strings). Example: --override "compare =
                        'mtime'". Override text is evaluated before *and* after the
                        config file however, the variables 'pre' and 'post' are
                        defined as True or False if it is before or after the config
                        file. These can be used with conditionals to control
                        overrides. See readme for details. Can specify multiple times.
                        There is no input validation so do not specify untrusted
                        inputs.

Listing Settings:
  --header, --no-header
                        Print a header where applicable. Default: True
  --head N              Include the first N lines plus --tail (if set).
  --tail N              Include --head (if set) plus the last N lines.
  --human               Use human readable sizes
  --timestamp-local     Specify timestamps in local time instead of UTC/Z (default).
                        Note, if applicable, all ModTimes are always local regardless

Time Specification:
  All TIMESTAMPs: Specify a date and timestamp in an ISO-8601 like format (YYYY-MM-
  DD[T]HH:MM:SS) with or without spaces, colons, dashes, "T", etc. Can optionally
  specify a numeric time zone (e.g. -05:00) or 'Z'. If no timezone is specified, it
  is assumed *local* time. Alternatively, can specify unix time with a preceding 'u'
  (e.g. 'u1678560662'). Or can specify a time difference from the current time with
  any (and only) of the following: second[s], minute[s], hour[s], day[s], week[s].
  Example: "10 days 1 hour 4 minutes 32 seconds". (The order doesn't matter). Can
  also specify "now" for the current time.

  --at, --before TIMESTAMP
                        Timestamp at which to show the files. If not specified, will
                        be the latest. Note that if '--after' is set, this will not be
                        the full snapshot in time.
  --after TIMESTAMP     Only show files after the specified time. Note that this means
                        the '--at' will not be the full snapshot.
  --only TIMESTAMP      Only show files AT the specified time. Shortcut for '--before
                        TIMESTAMP --after TIMESTAMP' since both are inclusive. Useful
                        if the exact timestamp is known such as from the 'timestamps'
                        command.

```

# prune


```text
usage: dfb prune [-h] [-v] [-q] [--temp-dir TEMP_DIR] --config file
                 [-o 'OPTION = VALUE'] [-n] [-i] [--dump FILE or -] [-N N]
                 [--subdir dir]
                 when

positional arguments:
  when                  Specify file modification prune time. The modification time of
                        a file is when the *next* file was written and not the
                        original timestamp. Specify a date and timestamp in an
                        ISO-8601 like format (YYYY-MM-DD[T]HH:MM:SS) with or without
                        spaces, colons, dashes, "T", etc. Can optionally specify a
                        numeric time zone (e.g. -05:00) or 'Z'. If no timezone is
                        specified, it is assumed *local* time. Alternatively, can
                        specify unix time with a preceding 'u' (e.g. 'u1678560662').
                        Or can specify a time difference from the current time with
                        any (and only) of the following: second[s], minute[s],
                        hour[s], day[s], week[s]. Example: "10 days 1 hour 4 minutes
                        32 seconds". (The order doesn't matter). Can also specify
                        "now" for the current time.

options:
  -h, --help            show this help message and exit
  -N, --keep-versions N
                        Specify number of versions to keep past the specified time.
                        This can be used to prune versions only. For example, to keep
                        only the last 10 versions, do "prune now -N 10". Can also be
                        combined with a date. For example, to keep the last 4 versions
                        older than 30 days, specify "prune '30 days' -N 4". Can also
                        specify negative numbers to shift forward in time (advanced
                        usage).
  --subdir dir          Prune only files in 'dir'. In order to ensure that references
                        do not break, this is mostly just a filter of what will be
                        pruned rather than a major performance enhancement.

Global Settings:
  Default verbosity is 1 for backup/restore/prune and 0 for listing

  -v, --verbose, --debug
                        +1 verbosity
  -q, --quiet           -1 verbosity
  --temp-dir TEMP_DIR   Specify a temp dir. Otherwise will use Python's default

Config & Cache Settings:
  --config file         (Required) Specify config file. Can also be specified via the
                        $DFB_CONFIG environment variable or is implied if executing
                        the config file itself. $DFB_CONFIG is currently not set.
  -o, --override 'OPTION = VALUE'
                        Override any config option for this call only. Must be
                        specified as 'OPTION = VALUE', where VALUE should be proper
                        Python (e.g. quoted strings). Example: --override "compare =
                        'mtime'". Override text is evaluated before *and* after the
                        config file however, the variables 'pre' and 'post' are
                        defined as True or False if it is before or after the config
                        file. These can be used with conditionals to control
                        overrides. See readme for details. Can specify multiple times.
                        There is no input validation so do not specify untrusted
                        inputs.

Execution Settings:
  Precedance follows the order specified in this help

  -n, --dry-run         Do not execute any changes
  -i, --interactive     Display planned actions and prompt to continue or stop
  --dump FILE or -      ADVANCED USAGE. Will dump the JSONL data that represents the
                        backup or prune. This can be used to manually do the action
                        and then with `dfb advanced dbimport` to apply. Note that this
                        is a more advanced form of --dry-run. If FILE ends in '.gz' or
                        '.xz', it will be compressed respectively. Can specify "-" to
                        print the dump to stdout.

Pruning takes into account reference files and delete markers that need to be kept.
Note that after pruning, it may appear possible to restore older than the prune time
but the results are very unlikely to be correct! It is due to files that are not-yet-
modified or are referenced. The prune algorithm may miss some delete makers that
technically could be deleted but it is more efficient not to try to identify them.

```

# summary


```text
usage: dfb summary [-h] [-v] [-q] [--temp-dir TEMP_DIR] --config file
                   [-o 'OPTION = VALUE'] [--at TIMESTAMP] [--after TIMESTAMP]
                   [--only TIMESTAMP]
                   [path]

positional arguments:
  path                  Starting path. Defaults to the top

options:
  -h, --help            show this help message and exit

Global Settings:
  Default verbosity is 1 for backup/restore/prune and 0 for listing

  -v, --verbose, --debug
                        +1 verbosity
  -q, --quiet           -1 verbosity
  --temp-dir TEMP_DIR   Specify a temp dir. Otherwise will use Python's default

Config & Cache Settings:
  --config file         (Required) Specify config file. Can also be specified via the
                        $DFB_CONFIG environment variable or is implied if executing
                        the config file itself. $DFB_CONFIG is currently not set.
  -o, --override 'OPTION = VALUE'
                        Override any config option for this call only. Must be
                        specified as 'OPTION = VALUE', where VALUE should be proper
                        Python (e.g. quoted strings). Example: --override "compare =
                        'mtime'". Override text is evaluated before *and* after the
                        config file however, the variables 'pre' and 'post' are
                        defined as True or False if it is before or after the config
                        file. These can be used with conditionals to control
                        overrides. See readme for details. Can specify multiple times.
                        There is no input validation so do not specify untrusted
                        inputs.

Time Specification:
  All TIMESTAMPs: Specify a date and timestamp in an ISO-8601 like format (YYYY-MM-
  DD[T]HH:MM:SS) with or without spaces, colons, dashes, "T", etc. Can optionally
  specify a numeric time zone (e.g. -05:00) or 'Z'. If no timezone is specified, it
  is assumed *local* time. Alternatively, can specify unix time with a preceding 'u'
  (e.g. 'u1678560662'). Or can specify a time difference from the current time with
  any (and only) of the following: second[s], minute[s], hour[s], day[s], week[s].
  Example: "10 days 1 hour 4 minutes 32 seconds". (The order doesn't matter). Can
  also specify "now" for the current time.

  --at, --before TIMESTAMP
                        Timestamp at which to show the files. If not specified, will
                        be the latest. Note that if '--after' is set, this will not be
                        the full snapshot in time.
  --after TIMESTAMP     Only show files after the specified time. Note that this means
                        the '--at' will not be the full snapshot.
  --only TIMESTAMP      Only show files AT the specified time. Shortcut for '--before
                        TIMESTAMP --after TIMESTAMP' since both are inclusive. Useful
                        if the exact timestamp is known such as from the 'timestamps'
                        command.

```

# advanced


```text
usage: dfb advanced [-h] command ...

options:
  -h, --help            show this help message and exit

Commands:
  Run `dfb advanced <command> -h` for help

  command
    dbimport            Import an exported list
    lsjson-import       Import an rclone lsjson result
    refresh-refs        Refresh reference entries in the local cache
    prune-file          Prune a specific file (real-path or rpath)
    timestamp-include-filters
                        Create rclone --include filters for a time range

```

# advanced dbimport


```text
usage: dfb advanced dbimport [-h] [-v] [-q] [--temp-dir TEMP_DIR] --config file
                             [-o 'OPTION = VALUE'] [--files [file ...]]
                             [--dirs [dir ...]] [--reset] [--upload]
                             [files ...]

[ADVANCED] Import file(s) and append to database. Will overwrite any existing data if
applicable. Note: Does *not* upload the import file lists to the remote as in a
backup.

positional arguments:
  files                 File(s) to import. Can be any rclone path including local.
                        Will automatically decompress .gz or .xz files

options:
  -h, --help            show this help message and exit
  --files [file ...]    File(s) to import. Can be any rclone path including local.
                        Will automatically decompress .gz or .xz files
  --dirs [dir ...]      Directories of files import. Can be any rclone path including
                        local. Will automatically decompress .gz or .xz files. Will
                        always import files then directories
  --reset               Reset the DB before import. Call without files to *just* reset
  --upload              Uploads the imported file(s). Will put them in a subdirectory
                        of the snapshots with the current time and label each file as
                        'N.{filename}'

Global Settings:
  Default verbosity is 1 for backup/restore/prune and 0 for listing

  -v, --verbose, --debug
                        +1 verbosity
  -q, --quiet           -1 verbosity
  --temp-dir TEMP_DIR   Specify a temp dir. Otherwise will use Python's default

Config & Cache Settings:
  --config file         (Required) Specify config file. Can also be specified via the
                        $DFB_CONFIG environment variable or is implied if executing
                        the config file itself. $DFB_CONFIG is currently not set.
  -o, --override 'OPTION = VALUE'
                        Override any config option for this call only. Must be
                        specified as 'OPTION = VALUE', where VALUE should be proper
                        Python (e.g. quoted strings). Example: --override "compare =
                        'mtime'". Override text is evaluated before *and* after the
                        config file however, the variables 'pre' and 'post' are
                        defined as True or False if it is before or after the config
                        file. These can be used with conditionals to control
                        overrides. See readme for details. Can specify multiple times.
                        There is no input validation so do not specify untrusted
                        inputs.

```

# advanced lsjson-import


```text
usage: dfb advanced lsjson-import [-h] [-v] [-q] [--temp-dir TEMP_DIR] --config file
                                  [-o 'OPTION = VALUE'] [--files [file ...]]
                                  [--dirs [dir ...]] [--reset] [--upload]
                                  [--refresh-references | --no-refresh-references]
                                  [files ...]

[ADVANCED] Import file(s) and append to database from rclone lsjson output. Unlike
`dbimport`, this expects rclone `lsjson` data (not dfb export JSONL). Will overwrite
any existing data if applicable. Note: Does *not* upload the import file lists to the
remote as in a backup.

positional arguments:
  files                 File(s) to import. Can be any rclone path including local.
                        Files must be JSON from rclone lsjson (array/object).

options:
  -h, --help            show this help message and exit
  --files [file ...]    File(s) to import. Can be any rclone path including local.
                        Files must be JSON from rclone lsjson (array/object).
  --dirs [dir ...]      Directories of files import. Can be any rclone path including
                        local. Will always import files then directories
  --reset               Reset the DB before import. Call without files to *just* reset
  --upload              Uploads the imported file(s). Will put them in a subdirectory
                        of the snapshots with the current time and label each file as
                        'N.{filename}'
  --refresh-references, --no-refresh-references
                        Whether or not to resolve imported reference files. Disabling
                        skips the resolver pass and keeps unresolved reference markers
                        in the cache. Default: True.

Global Settings:
  Default verbosity is 1 for backup/restore/prune and 0 for listing

  -v, --verbose, --debug
                        +1 verbosity
  -q, --quiet           -1 verbosity
  --temp-dir TEMP_DIR   Specify a temp dir. Otherwise will use Python's default

Config & Cache Settings:
  --config file         (Required) Specify config file. Can also be specified via the
                        $DFB_CONFIG environment variable or is implied if executing
                        the config file itself. $DFB_CONFIG is currently not set.
  -o, --override 'OPTION = VALUE'
                        Override any config option for this call only. Must be
                        specified as 'OPTION = VALUE', where VALUE should be proper
                        Python (e.g. quoted strings). Example: --override "compare =
                        'mtime'". Override text is evaluated before *and* after the
                        config file however, the variables 'pre' and 'post' are
                        defined as True or False if it is before or after the config
                        file. These can be used with conditionals to control
                        overrides. See readme for details. Can specify multiple times.
                        There is no input validation so do not specify untrusted
                        inputs.

```

# advanced refresh-refs


```text
usage: dfb advanced refresh-refs [-h] [-v] [-q] [--temp-dir TEMP_DIR] --config file
                                 [-o 'OPTION = VALUE']
                                 [--use-snapshots | --no-use-snapshots]

[ADVANCED] Refresh only reference entries already in the local cache. Unlike
`refresh`, this does not relist the destination; it only re-resolves reference marker
files using the same resolver path.

options:
  -h, --help            show this help message and exit
  --use-snapshots, --no-use-snapshots
                        Whether or not to also download snapshots from the destination
                        and update metadata before resolving references. Note that
                        snapshots are secondary but are faster for resolving
                        references. Default: True.

Global Settings:
  Default verbosity is 1 for backup/restore/prune and 0 for listing

  -v, --verbose, --debug
                        +1 verbosity
  -q, --quiet           -1 verbosity
  --temp-dir TEMP_DIR   Specify a temp dir. Otherwise will use Python's default

Config & Cache Settings:
  --config file         (Required) Specify config file. Can also be specified via the
                        $DFB_CONFIG environment variable or is implied if executing
                        the config file itself. $DFB_CONFIG is currently not set.
  -o, --override 'OPTION = VALUE'
                        Override any config option for this call only. Must be
                        specified as 'OPTION = VALUE', where VALUE should be proper
                        Python (e.g. quoted strings). Example: --override "compare =
                        'mtime'". Override text is evaluated before *and* after the
                        config file however, the variables 'pre' and 'post' are
                        defined as True or False if it is before or after the config
                        file. These can be used with conditionals to control
                        overrides. See readme for details. Can specify multiple times.
                        There is no input validation so do not specify untrusted
                        inputs.

```

# advanced prune-file


```text
usage: dfb advanced prune-file [-h] [-v] [-q] [--temp-dir TEMP_DIR] --config file
                               [-o 'OPTION = VALUE'] [-n] [-i] [--dump FILE or -]
                               [--error-if-referenced | --no-error-if-referenced]
                               rpath [rpath ...]

[ADVANCED] Prune a specific "real-path" (or "rpath") from the database, optionally
including all references to the path.

positional arguments:
  rpath                 Specify rpath(s) ("real paths") to prune. These are the paths
                        where the file is actually stored such as what you see with
                        `versions --real-path`

options:
  -h, --help            show this help message and exit
  --error-if-referenced, --no-error-if-referenced
                        If true (default), will error if there are references to the
                        provided path(s). If false, will *also* delete those
                        references. Default: True.

Global Settings:
  Default verbosity is 1 for backup/restore/prune and 0 for listing

  -v, --verbose, --debug
                        +1 verbosity
  -q, --quiet           -1 verbosity
  --temp-dir TEMP_DIR   Specify a temp dir. Otherwise will use Python's default

Config & Cache Settings:
  --config file         (Required) Specify config file. Can also be specified via the
                        $DFB_CONFIG environment variable or is implied if executing
                        the config file itself. $DFB_CONFIG is currently not set.
  -o, --override 'OPTION = VALUE'
                        Override any config option for this call only. Must be
                        specified as 'OPTION = VALUE', where VALUE should be proper
                        Python (e.g. quoted strings). Example: --override "compare =
                        'mtime'". Override text is evaluated before *and* after the
                        config file however, the variables 'pre' and 'post' are
                        defined as True or False if it is before or after the config
                        file. These can be used with conditionals to control
                        overrides. See readme for details. Can specify multiple times.
                        There is no input validation so do not specify untrusted
                        inputs.

Execution Settings:
  Precedance follows the order specified in this help

  -n, --dry-run         Do not execute any changes
  -i, --interactive     Display planned actions and prompt to continue or stop
  --dump FILE or -      ADVANCED USAGE. Will dump the JSONL data that represents the
                        backup or prune. This can be used to manually do the action
                        and then with `dfb advanced dbimport` to apply. Note that this
                        is a more advanced form of --dry-run. If FILE ends in '.gz' or
                        '.xz', it will be compressed respectively. Can specify "-" to
                        print the dump to stdout.

```

# advanced timestamp-include-filters


```text
usage: dfb advanced timestamp-include-filters [-h] [-v] [-q] [--temp-dir TEMP_DIR]
                                              --config file [-o 'OPTION = VALUE']
                                              [--at TIMESTAMP] [--after TIMESTAMP]
                                              [--only TIMESTAMP]
                                              [path]

Given a range of times specified, create and print rclone --include filters that can
be used for other rclone operations (e.g. ls, ncdu) on the destination. Note that the
filters are not perfect and could possibly include additional items if they happen to
have the same timestamp in the name

positional arguments:
  path                  Starting path. Defaults to the top.

options:
  -h, --help            show this help message and exit

Global Settings:
  Default verbosity is 1 for backup/restore/prune and 0 for listing

  -v, --verbose, --debug
                        +1 verbosity
  -q, --quiet           -1 verbosity
  --temp-dir TEMP_DIR   Specify a temp dir. Otherwise will use Python's default

Config & Cache Settings:
  --config file         (Required) Specify config file. Can also be specified via the
                        $DFB_CONFIG environment variable or is implied if executing
                        the config file itself. $DFB_CONFIG is currently not set.
  -o, --override 'OPTION = VALUE'
                        Override any config option for this call only. Must be
                        specified as 'OPTION = VALUE', where VALUE should be proper
                        Python (e.g. quoted strings). Example: --override "compare =
                        'mtime'". Override text is evaluated before *and* after the
                        config file however, the variables 'pre' and 'post' are
                        defined as True or False if it is before or after the config
                        file. These can be used with conditionals to control
                        overrides. See readme for details. Can specify multiple times.
                        There is no input validation so do not specify untrusted
                        inputs.

Time Specification:
  All TIMESTAMPs: Specify a date and timestamp in an ISO-8601 like format (YYYY-MM-
  DD[T]HH:MM:SS) with or without spaces, colons, dashes, "T", etc. Can optionally
  specify a numeric time zone (e.g. -05:00) or 'Z'. If no timezone is specified, it
  is assumed *local* time. Alternatively, can specify unix time with a preceding 'u'
  (e.g. 'u1678560662'). Or can specify a time difference from the current time with
  any (and only) of the following: second[s], minute[s], hour[s], day[s], week[s].
  Example: "10 days 1 hour 4 minutes 32 seconds". (The order doesn't matter). Can
  also specify "now" for the current time.

  --at, --before TIMESTAMP
                        Timestamp at which to show the files. If not specified, will
                        be the latest. Note that if '--after' is set, this will not be
                        the full snapshot in time.
  --after TIMESTAMP     Only show files after the specified time. Note that this means
                        the '--at' will not be the full snapshot.
  --only TIMESTAMP      Only show files AT the specified time. Shortcut for '--before
                        TIMESTAMP --after TIMESTAMP' since both are inclusive. Useful
                        if the exact timestamp is known such as from the 'timestamps'
                        command.

```

# utils


```text
usage: dfb utils [-h] command ...

options:
  -h, --help     show this help message and exit

Commands:
  Run `dfb utils <command> -h` for help

  command
    apath2rpath  Convert apparent path (apath) and date to a real path (rpath)
    rpath2apath  Convert real path (rpath) an apparent path (apath) and date
    tsconv       Convert between canonical filename timestamp tag formats

```

# utils apath2rpath


```text
usage: dfb utils apath2rpath [-h] [--date TIMESTAMP] [--format {datetag,base36-epoch}]
                             [-0]
                             files [files ...]

Convert apparent path (apath) and date to a real path (rpath)

positional arguments:
  files                 Specify one or more files. If '-' is specified, will read
                        stdin (and automatically handle newlines or null-bytes).

options:
  -h, --help            show this help message and exit
  --date TIMESTAMP      Specify timestamp for the filenames. Default is current time.
                        Specify a date and timestamp in an ISO-8601 like format (YYYY-
                        MM-DD[T]HH:MM:SS) with or without spaces, colons, dashes, "T",
                        etc. Can optionally specify a numeric time zone (e.g. -05:00)
                        or 'Z'. If no timezone is specified, it is assumed *local*
                        time. Alternatively, can specify unix time with a preceding
                        'u' (e.g. 'u1678560662'). Or can specify a time difference
                        from the current time with any (and only) of the following:
                        second[s], minute[s], hour[s], day[s], week[s]. Example: "10
                        days 1 hour 4 minutes 32 seconds". (The order doesn't matter).
                        Can also specify "now" for the current time.
  --format {datetag,base36-epoch}
                        Timestamp tag format to write in the generated rpath
  -0, --print0          Seperate multiple items with a null byte instead of newline

```

# utils rpath2apath


```text
usage: dfb utils rpath2apath [-h] [--timestamp-local] files [files ...]

Convert real path (rpath) an apparent path (apath) and date. Returns data in JSONLines
format with an ISO8601 date

positional arguments:
  files              Specify one or more files. If '-' is specified, will read stdin
                     (and automatically handle newlines or null-bytes).

options:
  -h, --help         show this help message and exit
  --timestamp-local  Return timestamps in local time instead of UTC

```

# utils tsconv


```text
usage: dfb utils tsconv [-h] [--to {datetag,base36-epoch}] [--raw] values [values ...]

Convert supported filename timestamp tags and emit both canonical forms.

positional arguments:
  values                One or more datetag or base36-epoch timestamp values

options:
  -h, --help            show this help message and exit
  --to {datetag,base36-epoch}
                        Output format used when --raw is specified
  --raw                 Print only the selected output token instead of JSONLines

```