# dfb - Dated File Backup

---

# WARNING: Early Public Beta

This is an **early public beta**. But, as discussed below, you don't need dfb to restore so the risk of usage is not too bad.

Please provide feedback!

---
---

Full-file, append-only, backups that can be easily restored to any point in time.

**The premise**: When a file is uploaded, the date is appended to the name. This allows you to see the state of the backup by only considering times <= a time of interest. Deletes are noted and moves are optionally tracked. 

Like its cousin, [rirb], dfb is not the most efficient, advanced, fast, featurefull,  sexy, or sophisticated. However, this approach is simple, easy to use, and easy to understand. No special tools are needed to restore and full copies of the files are stored as opposed to in chunks (which has pros and cons). For backups, I think these are great tradeoffs.

DFB came out of an idea I posed on the rclone forum and enabled by reusing a good bit of my other tool, RIRB.

## Project Goals:

* Be simple to understand, inspect, and even restore without the need for DFB itself. **The tool is only a convenience for restore; not a requirement**.
* Allow for rollback to any point-in-time as a first-class option (i.e. no crazy scripting)
* Support append-only/immutable storage natively

[rirb]:https://github.com/Jwink3101/rirb

## Command Help

See [CLI Help](CLI_help.md)

## File names

When files are backed up, they are renamed to have the date of the backup in the name. Filenames are

    <filename>.YYYYMMDDhhssmm<optional R or D>.ext

where the time is *always in UTC time*. When a file is modified at the source, it is copied to the remote in the above form. If it is deleted, it is a tiny file with `D` and if a file is moved, a reference, `R` is created pointing to the original. If moves are not tracked, then a move will generate a new copy of the file.

## Install

Just install from github directly. Note that a known issue (see below) is that you may get some PEP517 issues. This will be fixed shortly. You can add `--no-use-pep517`

    $ python -m pip install git+https://github.com/Jwink3101/dfb.git

## Setup

To start, run:

    $ dfb init path/to/config.py

The config file is **heavily and extensively documented**. It is read on Python without any sandboxing so make sure it is trusted. Some variables are defined inline including `os` and a few modules. The variables `__file__` and `__dir__` are `pathlib.Path` objects for the config file and the directory of the config file respectively. These can be used to specify paths to things like files for `--filter-from`.

The config file is heavily documented. The most important thing is setting the attributes. Unlike rclone, these are not done to defaults for each remote. For example, you shouldn't use `mtime` on WebDAV since it's not well supported.

Most comparisons are past-source-to-source but occasionally, such as after a `--refresh`, they are source-to-destination. In that case, it is possible to set different values.

Assuming this is a new setup, just run it:

    $ dfb backup --config path/to/config.py
    
or set the environment `$DFB_CONFIG_FILE=path/to/config.py`. Or, directly execute the config file (it has a custom shebang)

    ./config.py # <--- Assumed backup mode
    ./config.py backup
    ./config.py ls
    # ...

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

### Atomic Transfers

Most remote destinations, except local, sftp, and ftp, have atomic transfers. That means a file only shows up if it is complete. For the others, you can set

    dst_atomic_transfer = False
    
This will upload then move. Of course, this also breaks the ability to use object lock or other append-only storage systems but those are already atomic.

### Override

The config file can be overridden at the command line by specifying code to evaluate before *and* after the configuration file. In order to control if it is evaluated before *or* after the configuration file, the variables `pre` and `post` are defined. Consider the following example:

    dfb backup --config config.py -o "
        if post:
            filter_flags.extend(('--filter','- *.new'))"

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
    
## "FAQs"

(well, nobody is asking but you know what I mean...)

### What are the pros and cons of dated files vs full synthetic snapshots.

Synthetic snapshots, whether from chunk database tools like restic, Kopia, Borg, Duplicacy, etc or via hardlinks like Time Machine and rsync with `--link-dest`, are fundamentally different. They either keep a database of files and blocks or a full, hard-linked, directory structure.

This allows for pruning strategies like "keep one per week" but (a) it is risky to assume the one version of the file you care about is the one you keep, and (b) makes it fundamentally hard to delete specific files (though it *is* possible). dfb on the other hand keeps every version of every file up to the pruned cutoff. But because there is no singular snapshot, you can delete at will. Upload a large file by accident? Delete it on the dest and run with `--refresh` next time. That's it!

Furthermore, this approach means that you can also backup individual directories more often since you are just adding file versions. 

And, compared to the database tools, it is super easy to restore without the tool itself used to backup. Simple scripting will identify the needed transfers.

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

- PEP517 and other packaging warnings (should be fixed soon)
- `--links` doesn't work on local sources. This is an rclone issue. I may write a workaround or just wait for rclone. I haven't decided. See [rclone #6855](https://github.com/rclone/rclone/issues/6855)

**Roadmap**:

In no real particular order...

- Web-server, WebDAV server, and/or FUSE for backup. Potentially one that is just a loop-back to an rclone mount (letting rclone handle the details)
    - Alternative: symlink creation mode to an rclone mount
- [uncertain] Move file listing to the `rc` interface. Nearly everything else is rc-based for efficiency but file listing isn't. The code is there but it would require different exclusions to be set, it will be harder for the user, and doesn't really save much. Just one minor authorization call.
    - There is also a huge sunk-cost in rcloneapi.py. Oh well!
- Configurable date formats (dfb can actually read many formats but only write one.)
- Prune a subdir only.
- Additional documentation of different strategies and uses.
- Improved sqlite usage
- Make a copy of the local hash database. Make a way to import or use this. 
- Windows testing and likely (minor) fixing

### Why does some of the code look weird:

Simple answer: Make nobody happy. [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

### I pruned older files. Why do I see those timestamps?

Files can stick around after pruning for a few reasons. The most common is that it hasn't been updated after the prune time so it needs to stick around.

Another possible scenario is that a file is moved and the referrer is not pruned. In that case, both the referent and the delete file must stay. The former because it is still needed and the latter so that it doesn't appear undeleted. This does add additional complication but it is considered in the prune logic.