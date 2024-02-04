***

*Warning*: This tool is still in beta but (a) I've been using it heavily for 9 months without problems, (b) the design is such that even without it, you can get all of your files back, and (c) more testers are good! Use at your own risk but please also provide feedback

***


# dfb - Dated File Backup

The dfb backup tool utilizes rclone to create full-file, append-only backups that allow easy restoration to any point in time. Files are uploaded with dates appended to their names and deletes are noted with a delete marker. The design focuses on simplicity, easy understanding, and restoration without special tools. It prioritizes full-file backups stored natively on the remote, continuous rollback capability, straightforward backups and restores, and support for append-only storage. To accomplish this, it sacrifices some efficiency and advanced features. It may not be the the most efficient, advanced, fast, featurefull, sexy, or sophisticated backup tool these are great tradeoffs for peace-of-mind with backups! 

Many other popular backup tools/strategies including [macOS Time Machine][tm], [rsnapshot][rsnap], and rsync with `--link-dest` ([example][rs]) have the same (or even *worse*) tradeoffs and don't support cloud storage. Block-based backup tools like [restic][restic], [kopia][kopia], [borg][borg], and [Duplicacy][dup] offer increased efficiency and deduplication but are more prone to errors, more complicated, and require the tool to view and restore.

Design Tenets:

- **Easy to understand, interrogate, and restore**. The backup format is easily comprehended and can be reverse engineered simply. No special tools are needed to restore in theory (except if using crypt, you need to decrypt it). The format is about as straightforward as possible
- **Backup full copies of all files**. All files are full copies stored natively and can be downloaded right away, with or without special tools. This is *less efficient* than block-based tools, but that comes with the advantages noted above. Only exception is references for moved files which are easy to understand and use.
- **Restore to any point in time**. Can easily rollback to any point-in-time (unless pruned)
    - **Continuous in time is better than snapshots**. Many tools work off of synthetic snapshots. This does enable pruning like "keep 1 snapshot per week" but that is a risky approach. What if you need some file that falls in that range? Or what if you don't know when you modified a file? Instead, dfb can roll back to any point-in-time continuously *and* look at all versions of a specific file (with or without the tool itself to faciliate). Pruning capabilities let you specify a time and/or a number of versions to keep. 
- **Support append-only/immutable storage natively**. There is never a need to delete files except for pruning. Nothing ever gets renamed, deleted, or modified.


[tm]:https://support.apple.com/en-us/HT201250
[rsnap]:https://rsnapshot.org/
[rs]:https://web.archive.org/web/20230830063440/https://digitalis.io/blog/linux/incremental-backups-with-rsync-and-hard-links/
[restic]:https://restic.net/
[kopia]:https://kopia.io/
[borg]:https://www.borgbackup.org/
[dup]:https://duplicacy.com/

[rirb]:https://github.com/Jwink3101/rirb

## Command Help

See [CLI Help](CLI_help.md)

## File names

When files are backed up, they are renamed to have the date of the backup in the name. Filenames are

    <filename>.YYYYMMDDhhssmm<optional R or D>.ext

where the time is ***always in UTC (Z) time**. When a file is modified at the source, it is copied to the remote in the above form. If it is deleted, it is a tiny file with `D` and if a file is moved, a reference, `R` is created pointing to the original. If moves are not tracked, then a move will generate a new copy of the file.

Directory names are unchanged.

### References

The only exception to full-files is references. References write JSON data like[^refv1]:

```json
{"ver": 2, "rel": "<RELATIVE path to referenced file>"}
```

[^refv1]: Older versions were just a single line with the absolute path to the reference. This implied version 1 was less flexible and made for some other issues.

Note that references *are* considered and guarded when pruning (with associated tests). Be careful when pruning manually!

## Install

Just install from github directly.

    $ python -m pip install git+https://github.com/Jwink3101/dfb.git

## Setup

To start, run:

    $ dfb init path/to/config.py

The config file is **heavily and extensively documented**. It is read on Python without any sandboxing so make sure it is trusted. Some variables are defined inline including `os` and a few modules. The variables `__file__` and `__dir__` are `pathlib.Path` objects for the config file and the directory of the config file respectively. These can be used to specify paths to things like files for `--filter-from`.

For the most part, only the `src` and `dst` need to be set. Maybe `filter_flags`. The rest can be ignored or explored if interested

## Simple Usage

Assuming this is a new setup, just run it:

    $ dfb backup --config path/to/config.py
    
or set the environment `$DFB_CONFIG_FILE=path/to/config.py`. 

Or, directly execute the config file (it has a custom shebang):

    ./config.py # <--- Assumed backup mode
    ./config.py backup
    ./config.py ls
    # ...


### Override

The config file can be overridden at the command line by specifying code to evaluate before *and* after the configuration file. In order to control if it is evaluated before *or* after the configuration file, the variables `pre` and `post` are defined. Consider the following example:

    dfb backup --config config.py -o "
        if post:
            filter_flags.extend(('--filter','- *.new'))"


## Local Database

A local sqlite3 database is kept that serves to greatly speed up interaction. The database is used for listing the remote either in the CLI or for use in backups. It is updated as real-time as is possible with uploads so even interrupted or failed backups should be correct (or, if not, it may re-backup but not lose anything).

However, if the remote is modified manually, the database should be refreshed. Or if you wish to interact with the backup on another machine, it should either be refreshed or copied.

Note that, depending on the configuration, not all data may be in the database. For example, if not using `mtime` for `dst_compare`, it won't be fetched. That will still be there for restore if the remote supports it.

The database is stored in:

    <rclone cache dir>/DFB/<_uuid from config>.db

Where "`rclone cache dir`" is found from: `$ rclone config paths`. See also [Advanced Settings](docs/adv_settings.md).

When refreshing the database, dfb will also optionally read the logs of all uploaded files to fill in source-specific metadata and hashes. These are *secondary* to the operation of dfb and the native remote storage is sufficient to restore (with its own limitations).

## Pruning

You can prune dfb by removing *unneeded* files at snapshots older than a set time. It will keep files older than the specified time if they are still the most recent or are referenced. It can also be told to keep a certain number of versions of specific files.

This pruning strategy is discting from snapshot-based backups where you keep a certain number of snapshots, but offers a more practical approach as it doesn't risk losing important data bewtween snapshots and makes it very easy to understand changes.

Pruning is as simple as shown in some examples:

    $ dfb prune "365 days"
    $ dfb prune 2023-08-11
    $ dfb prune 2022-10-14 --keep-versions 10  # keeps 10 *additional* before the date
    $ dfb prune now --keep-versions 20         # just keep the last 20

Files *older* than the specified time or verion number may still exist if they are the latest or are referenced.

### Pruning individual files (Advanced)

Pruning individual files is supported through the `advanced` sub command. You need to know the "real-path" or "rpath" of the file. Assume `$DFB_CONFIG_FILE` is set for these examples.

For example, if you want to delete `my/large/file.ext`, you can do:

    $ dfb versions my/large/file.ext --real-path --ref-count
    
You will see a table of all versions of the file. Note that `--ref-count` is included since if you delete a referent (`Ref. Count > 1`), it will make the referrer appear deleted (that may be the intended outcome!). **Be careful about references**!

Simply identify the versions you wish to delete, copy the Real Path, and run

    $ dfb advanced prune-file my/large/file.<timestamp>.ext

You can, of course, do it all manually but (a) it won't check/delete references and (b) it won't refresh the database. To refresh after manual pruning, either call `refresh` or use the `advanced dbimport` command. See [notes on formats](docs/adv_backup_dump_format.md) for details.

## Additional Docs

<!--- BEGIN AUTO GENERATED -->
<!--- Auto Generated -->
<!--- DO NOT MODIFY. WILL NOT BE SAVED -->
- [(ADVANCED) Reading Backup and Prune `--dump` files](docs/adv_backup_dump_format.md)
- [Advanced (hidden) settings](docs/adv_settings.md)
- [Changelog](docs/changelog.md)
- [CLI Help](docs/CLI_help.md)
- [Using dfb with cold storage](docs/cold_storage.md)
- [Compare Setting Guidance](docs/compare_settings.md)
- ["FAQs"](docs/FAQs.md)
- [Symlink Restore](docs/symlink_restore.md)
<!--- END AUTO GENERATED -->

## Mount (EXPERIMENTAL)

In its own package is `dfb-mount` installed along with `dfb`. This is **EXPERIMENTAL** at best. It overlays *an rclone mount* and presents the latest (or set) version of the files.

Some notes:

- It does not serve files directly. It overlays an rclone mount. Use `rclone mount --vfs-cache-mode full` for best effect (especially since it will read the remote a lot). Rclone is way better suited for serving the files
- It is **stateless** (except for an optional cache). It doesn't use the remote database and need-not point to a single backup. You can mount many backups (or the top level of one with many backup destinations) and it'll work just fine. If it can't parse a date, it just provides the file and doesn't do any grouping.
- Unless using the `--remove-empty-dirs`, empty directories, such as from deleted files, will be shown. Determining if a directory is empty requires walking until it either finds a file or all the way until it doesn't. It is suggested to use the cache with this. You can always use a short cache duration.
- Logging is incomplete. I still need to fix this
- Does not resolve symlink files.
- Just to repeat, **THIS IS EXPERIMENTAL**. 
- Does **not work with libfuse3**. Use libfuse2:
    ```
    $ sudo apt-get update -y
    $ sudo apt-get install -y libfuse2
    ```

## Known Issues

- Logging in `dfb-mount` is not complete nor does it make much sense at the moment.
- Restore:
    - It is possible to create edge cases with symlink handling by having non-symlinks on the source names `<name>.rclonelink`. This is still mostly handled but can cause issues with some sources
    - Restore of symlinks is manual. See above for a Python snippet to rebuild links.
- Even with logs of files using source data, restore *always* directly uses the remote. Future versions may offer a secondary method to restore that information for certain remotes.

It is not really an "issue" per se, but one downside of this approach is that deleting a file adds a small file smaller than the block size of the system. And moving a file creates a delete and a new file.
