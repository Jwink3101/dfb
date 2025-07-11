# Changelog

(newest on top)

## 20250711.0BETA

- Improved bug fix from last version.
- Adds $DFB_DEBUG_RAISE_EXCEPTION environment variable to help debug by raising exception even if not in verbose mode

## 20250710.0BETA

- Updated readme
- *Bug Fix*: Fix handling of errors related to pruning already-deleted files.
- *Code*: Applied `isort` to all files

## 20250418.0BETA

- Adds "size" field to prune dump.

## 20250328.0BETA

- Added "R" to long listings of size for references. Cleaned up CLI text.
- Updated tests to fix issues with clone files (macOS) and a prior listing change (unrelated to the above)

## 20250216.0BETA

- Fixed a bug with converting times to local. Dfb was erroneously assuming any specified time was in the time zone of the computer/user *at that moment* and not the timezone of the computer/user for the given time. For example, if it was Standard Time but you gave a date in Daylight Savings Time, it would still make it standard time. This has no affect on anything but listings
- Updated readme

## 20241231.0BETA

- Fixed (and added test) bug where refreshing without a snapshot could cause an error.
- Changed ``$DFB_CONFIG_FILE` to `$DFB_CONFIG` but will fall back
- Add "Beta" to the version number

## 20241121.0

- Adds `summary` command which essentially aggregats timestamps.

Minor

- Fixed bug with `ls --real-path` to show the path of the reference marker rather than the referenced path. If you specify once, it shows the reference. Twice will show the referent.
- Changed default `--list / --list-only` option to be `files` if `--recursive`. Adds `--list` flag as shorter option. Adds "both"

## 20241016.0

- Adds `--real-path`  to `ls`
- Adds `utils apath2rpath` and `utils rpath2apath` command line utilities
- Internal: Better mapping of `rpath` to `apath` for some very particular edge cases. Associated tests.
    - Edge Case: Filenames without normal extensions but with dfb-style dates already. For example: `file.<date1>` will become `file.<dfb date>.<date1>` and round-trip correctly. This is counter how it may be done (incorrectly) manually but supporting manually tagged files is secondary.

## 20240923.0

- Adds `--recursive` and `--list-only` to `ls`

## 20240912.0

- Minor: Bug fix with `--subdir` and `empty_directory_markers = True`
- **NOTE**: There is a bug in rclone 1.68 with symlinks, `--copy-links` and macOS. For now, add `--local-no-clone` to the rclone flags.
    - [Forum Post](https://forum.rclone.org/t/macos-local-to-local-copy-with-copy-links-causes-error/47671)

## 20240829.0

- Minor: Adds timestamp to stats output
- Minor: Adds the ability to specify time ranges to the timestamp command
- Bug Fix: Respect `disable_prune` in `advanced prune-file`. Add test.

## 20240802.0

- Adds `advanced timestamp-include-filters` command to generate a list of rclone `--include` rules for a given timestamp range. Useful for things like running `ncdu` on the destination. 
    - Tests

(minor)

## 20240731.0

- Adds the ability to create empty directory markers at the destination. "Phantom" markers (`.dfbempty`) are created in the source listing with empty directories and then these are treated (and restored) like regular files. They can be deleted from the restore as a second action.
    - Tests for empty directories

## 20240620.0

All minor

- Adds a `path` parameter for timestamps
- Changed timeformat in listing commands to have a "T" instead of " " between date and time.
- Fixed missing `timestamps` command in docs

Also tests with rclone 1.67

## 20240608.0

- New command: `tree` to see a list of files backed up in a tree-like fashion
- Speed improvements to `ls` command by using more advanced dynamic query
- (Internal): Refactored snapshot commands to pass through a common util to dynamically build the query.

## 20240531.0

- Will now compress and upload snapshot files from incomplete runs (such as having errored, etc).
    - Snapshots are now written to the cache dir instead of temp dir so that they persist across failed runs.
- Adds `--upload` to `advanced dbimport` to upload the import files to the repository
- Minor Bug Fixes:
    - stats with no current transfers

These additions further hardens the snapshots as a *secondary* source of information. The files themselves remain ground-truth regardless!

## 20240428.0

- **Potentially Breaking**: Change the way dfb handles symlinks on local-to-local backups. See [symlink update document](symlink_update.md) for more details and how to convert the remote.
    -  **Important**: Setting `links` in the config is deprecated. Use the equivalent noted in symlink update document](symlink_update.md) in the `rclone_flags`.
    - This better matches rclone's default behavior, fixes the prior issues with restore, and closes #1

## 20240317.0

- Added option to specify a directory (including an rclone remote) of files to import for `advanced dbimport`.
- Improved stats on transfer and fixed bug where would start at 0 after some time.
- Minor fixes and improvements.

## 20240309.0

Minor!

- Added `DFB_OVERRIDE_TIMESTAMP` and `DFB_OVERRIDE_UNIXTIME` environment to override the current time. These should be used carefully!
- Documentation

## 20240208.0

- Moved advanced config items into regular config
- Added minimum size to reference vs upload again
- Bug fixes and sanity checks on smart splitting.

## 20240204.1 .. 20240204.5

- Snapshots are used to fill reference files. This is a MAJOR improvement if you have a lot of references since they all do not have to be read again
- snapshot output can be auto-compressed
- Bug Fixes

## 20240204.0

**BIG UPDATE**

This has some breaking changes. Please be careful.

- Adds `--export` to snapshot command
- Adds `--dump` to the backup and prune command. This is like the (now removed) shell script but includes more information.
- Adds `advanced dbimport` to allow you to import from an export. These two commands make it doable (though advanced) to use [cold or archival storage](cold_storage.md).
- `refresh` now downloads all snapshots and uses that to update the destination listing. These files are **secondary** to the actual listing (i.e. only used to update if possible). Note that this can be disabled with the flag `--no-refresh-use-snapshots` or `--no-use-snapshots` (depending on the command).
- Adds `--after` and `--only` to restore commands. Now you can restore only within a specific window
- Adds `--head` and `--tail` to listing commands
- **Important**: Adds `'auto'` for many compare attributes and makes them the default. Cleans up the config.
    - It is *strongly* suggested to redo your config to match the new format but the old will work just fine.
- **Potentially Breaking** (at least for some workflows): `--shell-script` for *uploads* are no longer a thing.
- **Potentially Breaking**: While `fail_shell` will still be called if the entire run fails, errors in uploads will get logged but are not explicitly going to make it get called.
- **Potentially Breaking**: Removed `reuse_hashes`. Please use [hasher remote](https://rclone.org/hasher/) for local hashing
- **Important**: Implemented a smarter extension splitting algorithm that more closely matches rclone's. After splitting the first extension, the remaining ones are also split iff they are valid MIME types. This means that files like `archive.tar.gz` will now become `archive.20240126094501.tar.gz` whereas before, it would be `archive.tar.20240126094501.gz`. Note that both will parse properly for existing files but going forward, new versions will use the updated split. When browsing the backup files directly, you may see both from before the split. 
    - Also made the date determination more robust for any manually placed files.
- **Potentially breaking**: Removed a few options in favor of being a bit more opinionated:
    - Always upload snapshots (in compressed form) -- These are also now automatically used to enhance refresh (Optional). 
        - Also uploads to dated dirs (but will download properly upon refresh for old ones)
    - Always upload logs
    - Stats settings

Prune:

- Adds `advanced prune-file` to manually prune files (and anything that references them)
- Made prune a bit more agressive at removing orphaned delete markers
- (*bug fix*) Prune now removes the reference files when it should. Previously, it would keep them and not be able to find the references upon refresh or restore. This wasn't critical because the broken reference *should* have been pruned!
- (*bug fix*) When using prune with `--subdir`, prune would *erroneously* remove referent files that were still being used outside of the subdir. 
- prune is now both more performant and uses less memory. (bonus in fixing the above)
    
Minor:

- Moved to Python logging
- Made `dfb.rclonerc` a bit more library-like.
- Try to avoid single-line help when executing config.
- Correct usage of size labels (e.g. MB vs MiB)
- Documentation of mtimes on restore
- More documentation moved to [docs](readme.md)
- Minor fix in logs for restore
- Moved from `_uuid` in config to `config_id` (with a different default) but will cleanly fall back to `_uuid` so nothing breaks
- Lots of code cleanup and documentation

## 20240106.0

- Added documentation that dfb-mount needs libfuse2 and will not work on libfuse3.
    ```
    $ sudo apt-get update -y
    $ sudo apt-get install -y libfuse2
    ```
- Added tool called `dfb-link` that will symlink the correct files. This can be used instead of dfb-mount if you do not want to use FUSE.
- Minor code changes to accommodate using the timestamp tool outside of dfb.

## 20231227.0

- `refresh` is now its own command and has been removed from all other commands except for `backup`
- Added better help for setting up attributes to readme

## 20230705.0

- Do not write debug to file except in testing. This was a leftover artifact that mostly didn't matter except when pruning a large backup, it was writing a line for 2-3x the number of files. 
- Better pypy support:
    - Added a dir creation that *shouldn't* be needed but seems to be under pypy. I will try to figure that out
    - Testing fails due to "open files". I think pypy should work but it is only lightly tested.

## 20230703.0

- Adds a `-N N, --keep-versions N` flag to prune. This lets you control versions more carefully and do things like:
    - Prune more than 15 copies: `prune now -N 15`
- Adds "`now`" as a valid timestamp.
- Adds the `ID:` to the log. Useful if messing with caches
- Adds "`total_size`" to `ls -ll`

## 20230630.0

- Removes some features that are no longer necessary with rclone 1.63:
    - Removes `dst_atomic_transfer` and supporting workaround. If you use a remote that is not atomic (e.g. sftp, ftp, local) and you do *not* want rclone to handle this, add `--inplace` to flags
    - see note below
- Adds a minimum rclone version and checks
- **Minimumum rclone version is 1.63**

Future versions will remove the special symlink handling but there are some bugs on the rclone side to be worked out first

## 20230623.0

- Handles the situation when using `compare = 'hash'` but the checksum/hash is still missing, as can happen with some remotes. Falls back to `'size'` compare similar to rclone's behavior.
    - Settable with `error_on_missing_hash` config
- More robust hash database even around missing hashes (unlikely to actually happen for the kind of remotes you would want to use a database but this should handle it.)
- Fixed bug with `reuse_hashes='size'` still caring about `mtime`

## 20230606.0

- Added the option to track moves and use a [server-side] copy instead of a reference. This only makes sense when the remote support server-side copy (not verified by dfb). It enables a cleaner representation without using bandwidth
- Tests for new capability

## 20230508.0

- Fixed removed feature.

## 20230507.0

- Adds the `disable_prune` (default to False) configuration option so that it is harder to accidentally prune. Makes it effectively append-only unless very, very explicit.
- Minor documentation updates

## 20230502.0

- Can specify `--deleted` (or `--del`) twice for `ls` and `snaphot` to *only* include deleted items

## 20230501.0

- Adds version count to `ls -ll`

## 20230429.0

- Adds the option to upload snapshot files with each run. These are functionally the same as calling `snapshot --only <timestamp> --deleted`
- Added `--deleted` to snapshot
- Bug Fixes
    - source and dest labels mixed up in restore logs

## 20230411.0 (BETA)

- Bug Fix

## 20230407.0 (BETA)

- If any part fails, it'll keep going until deletes and references happen
- Adds pruning only a subdir. Still have to compute all prunes to ensure there aren't references but will then filter them.

## 20230402.0 (BETA)

- Adds support for symlinks on a local source. It is not super robust but it works for most cases. Note that dfb does not restore the symlinks as symlinks. It restores them as `.rclonelink` files which can be easily turned into symlinks later. See an example in the readme

## 20230331.0 (BETA)

- **EXPERIMENTAL** FUSE mount that overlays an rclone mount (does not serve the remote directly)
- Cleanup, documentation, etc
- Fixed version numbering to validate PEP440 and other build errors.
    - This is still beta, but just isn't in the date-based version number

## 20230330.0.BETA

- Changed how references are written and saved. The change will not break existing references but older references will not work in the upcoming mounting tools
- Minor other updates

## 20230327.0.BETA

- 7-10x speed up of `ls` command, especially for larger backups.
- Readme clarity

## 20230326.0.BETA

Initial release