# Mount

The `mount` subcommand (`dfb mount`) overlays *an rclone mount* and presents the latest (or set) version of the files. It is optional and requires an extra dependency.

Some notes:

- It does not serve files directly. It overlays an rclone mount. Use `rclone mount --vfs-cache-mode full` for best effect (especially since it will read the remote a lot). Rclone is way better suited for serving the files
- It is **stateless** (except for an optional cache). It doesn't use the remote database and need-not point to a single backup. You can mount many backups (or the top level of one with many backup destinations) and it'll work just fine. If it can't parse a date, it just provides the file and doesn't do any grouping.
- Empty directory flags are grouped under **Empty Directory Handling** in `dfb mount --help`.
- `--remove-empty-dirs/--no-remove-empty-dirs` controls empty directory filtering. Default is `--no-remove-empty-dirs` because checking emptiness can be expensive.
- `--empty-check-depth N` limits how deep `--remove-empty-dirs` scans for visible files (depth 0 = only the directory itself). Lower values are faster but less certain. Ignored unless `--remove-empty-dirs` is set.
- `--empty-check-fail {hide,show}` controls what to do when `--empty-check-depth` cannot prove emptiness before hitting the depth limit. Default is `hide`.
- `--undated-files {show,hide,tag}` controls files without a datetag: `show` keeps them normal, `hide` excludes them, and `tag` appends `[NODATE]` (default). Empty directory detection uses this same visibility setting.
- Logging is incomplete. I still need to fix this
- Does not resolve symlink files.
- Requires `mfusepy` (`python -m pip install dfb[mount]` or `python -m pip install mfusepy`) and a working FUSE install. On macOS with FUSE-T, pass `--fuse-library fuse-t` (or a full library path) if detection fails.

## Install Requirements

macOS (FUSE-T):

```
$ brew install --cask fuse-t
$ python -m pip install dfb[mount]
```

macOS (macFUSE):

```
$ brew install --cask macfuse
$ python -m pip install dfb[mount]
```

Linux (libfuse3 or libfuse2):

```
$ sudo apt-get update -y
$ sudo apt-get install -y libfuse3
# If libfuse3 is unavailable, use libfuse2 instead:
# $ sudo apt-get install -y libfuse2
$ python -m pip install dfb[mount]
```

If FUSE-T is not auto-detected, run `dfb mount` with `--fuse-library fuse-t` (may be required).

## Usage

Linux:

```
$ rclone mount backups:server rclone_mount --vfs-cache-mode full --read-only
$ dfb mount rclone_mount dfb_mount
```

macOS (macFUSE or FUSE-T):

```
$ rclone mount backups:server rclone_mount --vfs-cache-mode full --read-only
$ dfb mount rclone_mount dfb_mount
```

When using FUSE-T and the library is not found, add `--fuse-library fuse-t` (may be required).
Use `-v/--verbose` for debug logs or `-q/--quiet` to suppress info logs.
Caching defaults to enabled when `--before/--at` is set and disabled otherwise; override with `--cache` or `--no-cache`.
Use `--show-deleted` to display deleted entries with a trailing `[DEL]`. Opening these files returns the literal text `[DEL]`. Directories may still be empty if you are viewing a time before files existed (not just because of deletions).
Use `--undated-files tag` to append `[NODATE]` to undated file names in the mounted view.
Use `--after` to filter to files after a timestamp, matching `dfb ls` behavior (note this is no longer a full snapshot at `--before/--at`).
