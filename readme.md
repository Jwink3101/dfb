***

> [!WARNING] 
> This tool is still in beta but (a) I've been using it heavily for 1+ years without problems, (b) the design is such that even without it, you can get all of your files back, and (c) more testers are good! Use at your own risk but please also provide feedback

***

# dfb - Dated File Backup

**Dated File Backup (dfb)** performs full-file, incremental backups by uploading new and modified files with the date appended, and using markers for deleted files. It can also optionally record references to renamed files. **It creates simple, reliable, self-documenting, portable, durable, and auditable backups.** It does not rely on any proprietary or complex formats or tools, and backups can be easily understood and restored manually. It uses [`rclone`](https://rclone.org/) to connect to and from a variety of cloud providers, offering additional utility such as encryption and enabling backups of cloud storage. However, due to its simple, full-file nature, it is not ideal for large files with frequent small changes.

Many popular backup tools — such as [macOS Time Machine][tm], [rsnapshot][rsnap], or `rsync` with `--link-dest` ([example][rs]) — work well for local snapshots but often lack robust cloud support and portability. Tools like [restic][restic] and [kopia][kopia] offer greater efficiency through deduplication and compression, but rely on complex tooling, opaque formats, and can be difficult to audit or restore without the original software.

**dfb offers a simpler, more transparent approach:**

* **Straightforward full-copy backups** — new and modified files are stored with date-tagged filenames
* **No proprietary formats** — everything is human-readable and restorable with basic tools
* **Cloud-ready** — uses `rclone` to support many storage providers, with optional encryption 🔐. Can also support immutable/WORM/append-only storage.
* **Reliable and auditable** — no hidden indexes, no fragile snapshots, no surprises

> [!CAUTION] 
> dfb is not ideal for large files with frequent small changes, as it stores full copies.

Choose **dfb** if you value **clarity, resilience, and control** over aggressive storage optimization.


[tm]:https://support.apple.com/en-us/HT201250
[rsnap]:https://rsnapshot.org/
[rs]:https://web.archive.org/web/20230830063440/https://digitalis.io/blog/linux/incremental-backups-with-rsync-and-hard-links/
[restic]:https://restic.net/
[kopia]:https://kopia.io/


## Command Help

See [CLI Help](CLI_help.md)

## File names

When files are backed up, they are renamed to include the date of the backup in the name. Filenames are:

    <filename>.YYYYMMDDhhssmm<optional R or D>.ext

where the time is **_always_ in UTC (Z) time**. When a file is modified at the source, it is copied to the remote in the above form. If it is deleted, it is marked with a tiny file with `D`. If a file is moved, a reference `R` is created pointing to the original. If moves are not tracked, then a move will generate a new copy of the file.

There may also be empty directory markers named `.dfbempty.<date>` which are used to hold the empty directory on the destination.

Directory names remain unchanged.

### Reference Files

The only exception to full-file backups are references. References write JSON data like[^refv1]:

```json
{"ver": 2, "rel": "<RELATIVE path to referenced file>"}
```

[^refv1]: Older versions were just a single line with the absolute path to the reference. This implied version 1 was less flexible and made for some other issues.

Note that references *are* considered and guarded when pruning (with associated tests). Be careful when pruning manually so as to not break a reference file.

The reference file is "ground truth" but some additional files are also backed up to help speed up refreshes without having to read every reference file.

## Install

From PyPI:

    $ pip install dfb
    
or

    $ pip install -U dfb


Just install from github directly.

    $ python -m pip install git+https://github.com/Jwink3101/dfb.git

## Setup

To start, run:

    $ dfb init path/to/config.py

The config file is **heavily and extensively documented**. It is read in Python without any sandboxing so make sure it is trusted! Some variables are defined inline including `os` and a few modules. The variables `__file__` and `__dir__` are `pathlib.Path` objects for the config file and the directory of the config file respectively. These can be used to specify paths to things like files for `--filter-from` rclone flag.

## Simple Usage

Assuming this is a new setup, just run it:

    $ dfb backup --config path/to/config.py
    
Or set the environment:

```bash
export DFB_CONFIG=path/to/config.py
$ dfb backup
```

Or, directly execute the config file (it has a custom shebang):

    ./config.py # <--- Assumed backup mode
    ./config.py backup
    ./config.py ls
    # ...


### Override

The config file can be overridden at the command line by specifying code to evaluate before *and* after the configuration file. In order to control if it is evaluated before *or* after the configuration file, the variables `pre` and `post` are defined.

## Local Database

A local sqlite3 database is kept that serves to greatly speed up interaction. The database is used for listing the remote either in the CLI or for use in backups. It is updated as real-time as is possible with uploads so even interrupted or failed backups should be correct (or, if not, it may re-backup but not lose anything).

However, if the remote is modified manually, the database should be refreshed. Or if you wish to interact with the backup on another machine, it should either be refreshed or copied.

Note that, depending on the configuration, not all data may be in the database. For example, if not using `mtime` for `dst_compare`, it won't be fetched. That will still be there for restore if the remote supports it.

The database is stored in:

    <rclone cache dir>/DFB/<config_id from config>.db

Where "`rclone cache dir`" is found from: `$ rclone config paths`. See also [Advanced Settings](docs/adv_settings.md).

When refreshing the database, dfb will also optionally read the logs of all uploaded files to fill in source-specific metadata and hashes and speed up references. These are *secondary* to the operation of dfb and the native remote storage is sufficient to restore (up to the limitations of the remote).

## Pruning

You can prune dfb by removing *unneeded* files at snapshots older than a set time. The date specified is the oldest point-in-time where you can restore fully. Files older than that time may still be present as needed.

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
- [Changelog](docs/changelog.md)
- [CLI Help](docs/CLI_help.md)
- [Using dfb with cold storage](docs/cold_storage.md)
- [Compare Setting Guidance](docs/compare_settings.md)
- ["FAQs"](docs/FAQs.md)
- [Mount (EXPERIMENTAL)](docs/mount.md)
- [Symlink Update](docs/symlink_update.md)
<!--- END AUTO GENERATED -->

## Known Issues

- Logging in `dfb-mount` is not complete nor does it make much sense at the moment.
- Even with logs of files using source data, restore *always* directly uses the remote. Future versions may offer a secondary method to restore that information for certain remotes.
    - Example: If the remote doesn't support mtime, even if mtime is used for backups based on the logs and local database, restore will not restore mtime.
- Deletes are marked with a 3 byte file (just ASCII encoded `DEL`). This is smaller than most block sizes so they take more space on a local filesystem.
- It is assumed that the source does not change from when listing to backing up. If a file is deleted in the iterim, it could cause an error. If it is modified, there may be a future false-positive (which is a safe scenario).
