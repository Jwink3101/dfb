# dfb - Dated File Backup

---

# WARNING: Public Beta

This is a ** public beta**. But, as discussed below, you don't need dfb to restore so the risk of usage is not too bad.

Please provide feedback!

---
---

Full-file, append-only, backups that can be easily restored to any point in time. Can back up from and send to *any** [rclone](https://rclone.org/) remote.

**The premise**: When a file is uploaded, the date is appended to the name. This allows you to see the state of the backup by only considering times <= a time of interest. Deleted files are represented with a delete marker. Optionally, moves can be tracked with references.

Like its cousin, [rirb][rirb], dfb is not the most efficient, advanced, fast, featurefull,  sexy, or sophisticated. However, this approach is **simple, easy to use, and easy to understand**. No special tools are needed to restore and full copies of the files are stored as opposed to in chunks (which has pros and cons). For backups, I think these are great tradeoffs.

## Project Goals:

* Be simple to understand, inspect, and even restore without the need for dfb itself. **The tool is only a convenience for restore; not a requirement**.
    * By design, even without documentation, the format can be easily reverse engineered.
* Allow for rollback to any point-in-time as a first-class option (i.e. no crazy scripting)
* Support append-only/immutable storage natively

[rirb]:https://github.com/Jwink3101/rirb

## Command Help

See [CLI Help](CLI_help.md)

## File names

When files are backed up, they are renamed to have the date of the backup in the name. Filenames are

    <filename>.YYYYMMDDhhssmm<optional R or D>.ext

where the time is *always in UTC (Z) time*. When a file is modified at the source, it is copied to the remote in the above form. If it is deleted, it is a tiny file with `D` and if a file is moved, a reference, `R` is created pointing to the original. If moves are not tracked, then a move will generate a new copy of the file.

## Install

Just install from github directly.

    $ python -m pip install git+https://github.com/Jwink3101/dfb.git

## Setup

To start, run:

    $ dfb init path/to/config.py

The config file is **heavily and extensively documented**. It is read on Python without any sandboxing so make sure it is trusted. Some variables are defined inline including `os` and a few modules. The variables `__file__` and `__dir__` are `pathlib.Path` objects for the config file and the directory of the config file respectively. These can be used to specify paths to things like files for `--filter-from`.

The config file is heavily documented. The most important thing is setting the attributes. Unlike rclone, these are not done to defaults for each remote. For example, you shouldn't use `mtime` on WebDAV since it's not well supported.

Most comparisons are past-source-to-source but occasionally, such as after a `--refresh`, they are source-to-destination. In that case, it is possible to set different values.

Assuming this is a new setup, just run it:

    $ dfb backup --config path/to/config.py
    
or set the environment `$DFB_CONFIG_FILE=path/to/config.py`. 

Or, directly execute the config file (it has a custom shebang):

    ./config.py # <--- Assumed backup mode
    ./config.py backup
    ./config.py ls
    # ...

## Local Database

A local sqlite3 database is kept that serves to greatly speed up interaction. The database is used for listing the remote either in the CLI or for use in backups. It is updated as real-time as is possible with uploads so even interrupted or failed backups should be correct (or, if not, it may re-backup but not lose anything).

However, if the remote is modified manually, the database should be refreshed. Or if you wish to interact with the backup on another machine, it should either be refreshed or copied.

Note that, depending on the configuration, not all data may be in the database. For example, if not using `mtime` for `dst_compare`, it won't be fetched. That may still be there for restore.

The database is stored in:

    <rclone cache dir>/DFB/<_uuid from config>.db

Where "`rclone cache dir`" is found from: `$ rclone config paths`
    

## Configuration

### Comparison and Rename Attributes

The attributes for comparison and for renames are user settable. If both remotes support hashes, it almost always best to use them. And if the source is slow to list ModTime, you can also set `get_modtime = False`. If remotes support ModTime and it is fast, that is a decent choice for both compare and renames.

Generally speaking, comparisons and renames are actually source-to-source because the source values are saved. However, if run with `--refresh`, then comparisons and move-tracking are source-to-dest. In that case, you can set `dst_compare` and `dst_renames`. 

Examples:

**Local to WebDAV** and **Local to S3**

WebDAV doesn't support ModTime and S3 does but it is super slow

```python
compare = 'mtime'
dst_compare = 'size'
renames = 'mtime'
dst_renames = False
```
This will disable rename tracking when using `--refresh` since size is not a good rename tracker.

**S3 to S3**:

Use hashes since they both support it

```python
compare = 'hash'
dst_compare = None
renames = 'hash'
dst_renames = None
```

**S3 to Local**

Use hashes for itself

```
compare = 'hash'
dst_compare = 'size'
renames = 'hash'
dst_renames = False
```

### Override

The config file can be overridden at the command line by specifying code to evaluate before *and* after the configuration file. In order to control if it is evaluated before *or* after the configuration file, the variables `pre` and `post` are defined. Consider the following example:

    dfb backup --config config.py -o "
        if post:
            filter_flags.extend(('--filter','- *.new'))"

## Symlinks

For local sources, you can configure how to handle links. Due to an [rclone bug #6855](https://github.com/rclone/rclone/issues/6855), dfb handles symlinks on its own for backups. This works for both the regular and `--shell-script` modes. However, dfb *does not support restore of symlinks*. After restore, there will be `.rclonelink` files that used to be symlinks. These can be manually (or simply Python script) fixed later:

```python
from pathlib import Path

RESTORE_DIR = "."

links = Path(RESTORE_DIR).rglob("*.rclonelink")
for link in links:
    dst = str(link).removesuffix(".rclonelink")
    src = link.read_text()
    Path(dst).symlink_to(src)
    # link.unlink()  # OPTIONAL
```

Note that this will fail if for some reason there are files called `.rclonelink` that aren't intended to be links

## Pruning

You can prune dfb by removing *unneeded* files at snapshots older than a set time. At it's most basic form, the pruning algorithm is simply to delete all files older than the specified time *except* the last one. This is demonstrated below.

    (A) --- (B) --- (C) -|- (D) --- (E) --- (F)
                       prune

In this simple example, pruning at the noted spot will delete `(A)` and `(B)` but *not* `(C)`. However, references from moving files adds some complexity to this and dfb is designed to handle it.

### Pruning individual files (manually)

There is no way inside of dfb to prune individual files but part of the design of the tool enables you to manually delete any version you wish of a file. These manual operations are fully supported and enable more control. There is no need to "rewrite" history.

For example, if you want to delete `my/large/file.ext`, you can do:

    $ dfb versions my/large/file.ext --real-path --ref-count
    
You will see a table of all versions of the file. Note that `--ref-count` is included since if you delete a referent (`Ref. Count > 1`), it will make the referrer appear deleted (that may be the intended outcome!). 

Simply identify the versions you wish to delete, copy the Real Path, and run

    $ rclone delete myremote:my/large/file.<timestampe>.ext

Afterwards, you should refresh the file listings. Most calls can have a `--refresh` but a simple one is just

    $ dfb ls --refresh

## Mount (EXPERIMENTAL)

In its own package is `dfb-mount` installed along with `dfb`. This is **EXPERIMENTAL** at best. It overlays *an rclone mount* and presents the latest (or set) version of the files.

Some notes:

- It does not serve files directly. It overlays an rclone mount. Use `rclone mount --vfs-cache-mode full` for best effect (especially since it will read the remote a lot). Rclone is way better suited for serving the files
- It is **stateless** (except for an optional cache). It doesn't use the remote database and need-not point to a single backup. You can mount many backups (or the top level of one with many backup destinations) and it'll work just fine. If it can't parse a date, it just provides the file and doesn't do any grouping.
- Unless using the `--remove-empty-dirs`, empty directories, such as from deleted files, will be shown. Determining if a directory is empty requires walking until it either finds a file or all the way until it doesn't. It is suggested to use the cache with this. You can always use a short cache duration.
- Logging is incomplete. I still need to fix this
- Does not resolve symlink files.
- Just to repeat, **THIS IS EXPERIMENTAL**. 

## "FAQs"

(well, nobody is asking but you know what I mean...)

### What are the pros and cons of dated files vs full synthetic snapshots.

Synthetic snapshots, whether from chunk database tools like restic, Kopia, Borg, Duplicacy, etc or via hardlinks like Time Machine and rsync with `--link-dest`, are fundamentally different. They either keep a database of files and blocks or a full, hard-linked, directory structure.

This allows for pruning strategies like "keep one per week" but (a) it is risky to assume the one version of the file you care about is the one you keep, and (b) makes it fundamentally hard to delete specific files (though it *is* possible). dfb on the other hand keeps every version of every file when modified up to the pruned cutoff. But because there is no singular snapshot, you can delete at will. Upload a large file by accident? Delete it on the dest and run with `--refresh` next time. That's it!

Other advantages of this approach are:

- You can also backup individual directories more often since you are just adding file versions. Synthetic snapshot tools may deduplicate the data but keeps it as its own backup series
- You can very easily and *natively* see all version of a file. With synthetic backups, it can be done but is harder (depending on the approach).
    - dfb provides a UI for this but it can also be done manually
- Compared to the database tools, it is super easy to restore without the tool itself used to backup. Simple scripting will identify the needed transfers.

### How is this better than native versioning on some remotes

Remotes like OneDrive and consumer storage that offer versioning usually only do it via the website and requires you manually restore for each file. Miserable experience though it'll do in a pinch.

However, some remotes for rclone, like B2 and S3, offer native versions with built in flags in rclone that let you do things like dfb. Are they better? Maybe. They are different and to each their own. Personally, I like to *own* my backup process and not rely on the backend storage. I also like the freedom to move storage if I need, both capability and backup migration.

But the real answer is to use both! When you prune (or get hacked/randomwared), you have another backup!

### Why only keep 1 second precision?

Simple answer: Tradeoff of compactness and precision. Long filenames can be tough on some remotes and, in reality, there isn't much of a use case for < 1s level of precision.

As an aside, I considered other options. I considered adding Z to the timestamp to be explicit on timezone but decided it wasn't worth it. I tried shortening it by using unix time which is nice but hard to parse. I even tried encoding the filenames with letters but
that makes it even *harder* to human parse. It is not recommended but in practice any ISO8601 formatted date, with and without timezones, will be parsed (with omitted timezones assumed UTC).

### What happens if a transfer is interrupted.

**Short answer:** Run it again and it'll be fine! You will lose the progress of active transfers but they will be fixed and it'll keep going.

Long answer: Incomplete versions may show up but will, by definition, a new version will be uploaded next time. It is also possible if an interruption is in the very short time between upload and saving in the database, that a complete version will be there and you upload again.

Note, unlike [rirb][rirb], there is *no need to refresh next time* because the files themselves define the state and the local cache is updated per-file.

### What are the known issues and what is on the roadmap

**Known Issues**:

- Logging in `dfb-mount` is not complete nor does it make much sense at the moment.
- Restore:
    - It is possible to create edge cases with symlink handling by having non-symlinks on the source names `<name>.rclonelink`. This is still mostly handled but can cause issues with some sources
    - Restore of symlinks is manual. See above for a Python snippet to rebuild links.
- (work in progress) Metadata isn't preserved on B2 but works via the S3 API

**Roadmap**:

In no real particular order...

- Web-server, WebDAV server, and/or ~~FUSE~~ (DONE) for backup. Potentially one that is just a loop-back to an rclone mount (letting rclone handle the details)
    - Alternative: symlink creation mode to an rclone mount
    - Mount TODO
        - [ ] Logging
            - [ ] FUSE code
            - [ ] Mount Code
- [uncertain] Move file listing to the `rc` interface. Nearly everything else is rc-based for efficiency but file listing isn't. The code is there but it would require different exclusions to be set, it will be harder for the user, and doesn't really save much. Just one minor authorization call.
    - There is also a huge sunk-cost in rcloneapi.py. Oh well!
- Configurable date formats (dfb can actually read many formats but only write one.)
- Prune a subdir only.
- Additional documentation of different strategies and uses.
- Improved sqlite usage
- Make a copy of the local hash database. Make a way to import or use this. 
- Windows testing and likely (minor) fixing

### Why does some of the code look weird:

Simple answer: [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

I don't love it but it keeps the style consistent. One way to look at a compromise is each side is equally unhappy!

### I pruned older files. Why do I see those timestamps?

Files can stick around after pruning for a few reasons. The most common is that it hasn't been updated after the prune time so it needs to stick around.

Another possible scenario is that a file is moved and the referrer is not pruned. In that case, both the referent and the delete file must stay. The former because it is still needed and the latter so that it doesn't appear undeleted. This does add additional complication but it is considered in the prune logic.