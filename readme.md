***

*Warning*: This tool is still in beta but (a) I've been using it heavily for 1+ years without problems, (b) the design is such that even without it, you can get all of your files back, and (c) more testers are good! Use at your own risk but please also provide feedback

***


# dfb - Dated File Backup

---

**BLUF**:

- **Dated File Backup (dfb)**: Offers full-file, incremental backups with date-appended filenames for easy tracking and retrieval.
- **Simplicity and Transparency**: Allows manual verification and restoration without needing the tool or extensive documentation, reducing reliance on proprietary systems.
- **Broad Compatibility**: Supports various storage options via rclone, including WORM/immutable/append-only remotes for enhanced security.
- **Comparison with Other Solutions**: Unlike complex tools like restic and kopia, dfb avoids error-prone processes and supports seamless cloud backups better than Time Machine or rsnapshot.
- **Limitations**: Not ideal for large files with frequent small changes due to full copy storage.
- **Overall Benefit**: Provides a straightforward, reliable backup solution prioritizing simplicity and user control over data.

---

Dated File Backup (dfb) is a simple yet powerful tool for full-file, incremental backups. With dfb, every new or modified file is uploaded with the date appended to the filename, ensuring easy tracking and retrieval. Deleted files are noted with a small dated delete marker, and optionally, renamed files can be referenced. All files are stored as full copies directly on the remote server.

**Simplicity is dfb's greatest strength.** Unlike more complex tools, dfb allows backups to be manually understood, verified, and restored to any point in time without needing the tool itself or a deep understanding of backup formats. The format can be easily deciphered without the need for additional documentation. This transparency ensures peace of mind and reduces reliance on proprietary systems.

Utilizing rclone as an interface to both source and destination, dfb supports a wide range of storage options, enabling seamless cloud-to-cloud backups. Additionally, it natively supports WORM/immutable/append-only destination remotes, enhancing data security.

Many popular backup solutions, such as [macOS Time Machine][tm], [rsnapshot][rsnap], and rsync with `--link-dest` ([example][rs]), have similar or even greater limitations, particularly in cloud storage support. Block-based, cloud-native tools like [restic][restic] and [kopia][kopia] offer efficiency and deduplication but are often more error-prone, complex, and difficult to restore or verify without the original tool.

With dfb, there is no need for special initial backups or periodic snapshots, simplifying the backup process. This straightforward approach reduces the risk of errors and ensures that backups are always accessible and verifiable.

Choose dfb for a backup solution that prioritizes simplicity, transparency, and reliability, giving you full control over your data without the complexities of more sophisticated systems. However, dfb may not be ideal for large files with frequent small changes, as it stores full copies. For most needs, dfb provides a reliable, transparent backup solution without the complexities of more sophisticated systems.

[tm]:https://support.apple.com/en-us/HT201250
[rsnap]:https://rsnapshot.org/
[rs]:https://web.archive.org/web/20230830063440/https://digitalis.io/blog/linux/incremental-backups-with-rsync-and-hard-links/
[restic]:https://restic.net/
[kopia]:https://kopia.io/

## Design Tenets:

- **Easy to understand, verify, interrogate, and restore**. The backup format is easy to comprehend and can be reverse-engineered simply. No special tools, including dfb itself, are needed to restore (except if using crypt, you need to decrypt it). The format is about as straightforward as possible!
- **Backup full copies of all files**. All files are full copies stored natively and can be downloaded right away (and manually if needed). This is *less efficient* than block-based tools, but it comes with the advantages noted above.
- **Restore to any point in time**. You can easily roll back to any point in time in the backup.
    - **Continuous in time is better than snapshots**. Many tools work off [synthetic] snapshots. Snapshots enable pruning like "keep 1 snapshot per week," but that is a risky approach. What if you need some file that falls in that range? Or what if you don't know when you modified a file? Instead, dfb can roll back to any point in time continuously *and* look at all versions of a specific file (with or without the tool itself to facilitate). Dfb can still be pruned of "prune files older than XYZ days" and/or "prune all but the last N versions."
- **Support append-only/immutable storage natively**. There is never a need to delete files except for pruning. Nothing ever gets renamed, deleted, or modified.


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

## Install

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
