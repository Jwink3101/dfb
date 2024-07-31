# "FAQs"

(well, nobody is asking but you know what I mean...)

## What are the pros and cons of dated files vs full synthetic snapshots.

Synthetic snapshots, whether from chunk database tools like restic, Kopia, Borg, Duplicacy, etc or via hardlinks like Time Machine and rsync with `--link-dest`, are fundamentally different. They either keep a database of files and blocks or a full, hard-linked, directory structure.

This allows for pruning strategies like "keep one per week" but (a) it is risky to assume the one version of the file you care about is the one you keep, and (b) makes it fundamentally hard to delete specific files (though it *is* possible). dfb on the other hand keeps every version of every file when modified up to the pruned cutoff. But because there is no singular snapshot, you can delete at will. Upload a large file by accident? Delete it on the dest and run with `--refresh` next time. That's it!

Other advantages of this approach are:

- You can also backup individual directories more often since you are just adding file versions. Synthetic snapshot tools may deduplicate the data but keeps it as its own backup series
- You can very easily and *natively* see all version of a file. With synthetic backups, it can be done but is harder (depending on the approach).
    - dfb provides a UI for this but it can also be done manually
- Compared to the database tools, it is super easy to restore without the tool itself used to backup. Simple scripting will identify the needed transfers.

## How is this better than native versioning on some remotes

Remotes like OneDrive and consumer storage that offer versioning usually only do it via the website and requires you manually restore for each file. Miserable experience though it'll do in a pinch.

However, some remotes for rclone, like B2 and S3, offer native versions with built in flags in rclone that let you do things like dfb. Are they better? Maybe. They are different and to each their own. Personally, I like to *own* my backup process and not rely on the backend storage. I also like the freedom to move storage if I need, both capability and backup migration.

But the real answer is to use both! When you prune (or get hacked/randomwared), you have another backup!

## Why only keep 1 second precision?

Simple answer: Tradeoff of compactness and precision. Long filenames can be tough on some remotes and, in reality, there isn't much of a use case for < 1s level of precision.

As an aside, I considered other options. I considered adding Z to the timestamp to be explicit on timezone but decided it wasn't worth it. I tried shortening it by using unix time which is nice but hard to parse. I even tried encoding the filenames with letters but
that makes it even *harder* to human parse. It is not recommended but in practice any ISO8601 formatted date, with and without timezones, will be parsed (with omitted timezones assumed UTC).

## Why refresh with snapshots? Why not *just* use snapshots?

dfb stores a changelog or snapshot in `dst:.dfb/snapshots`. These are based on the *source* listing. The reason they are used is to update the destination with source metadata. For example, a local source may have ModTime but a WebDAV destination does not. In that case, you would use `dst_compare` (likely set to `'size'`). But the snapshot makes it so that upon a refresh, you still have that data. 

The reason we do not *just* use snapshots is that it is that they should be treated as *secondary* to the backup. The files are the backup! Anything in the snapshot and not in the backup, will get ignored.

With that said, you can use `dfb advanced dbimport` to *just* import from data.

## When I restore, I get extra `.dfbempty` files

The way dfb handles empty directories if enabled (disabled by default) is to create directory markers at the destination. Essentially, these are phantom source files in empty directories. Restore does *not* handle these differently (i.e. using mkdir and not copying). To delete them after a restore, simply run:

    $ rclone delete -v --include ".dfbempty" remote:

## Why not use the date in the root folder?

Rather than `path/to/file.<date>.ext`, dfb could have done `<date>path/to/file.ext`. I strongly considered that and it would have made some things easier but the problem is that (a) interrogating the backup manually would have been *much* harder and walking the file system could have been much more expensive!

## Don't you end up with a lot of files in a directory?

Yep. Certainly possible and that is a tradeoff. One way to deal with it is to split is manually and use [rclone union](https://rclone.org/union/) to join them. While some remotes get uncomfortable with too many files, rclone can handle it just fine.

It also would make it harder to manually inspect the backed up files and compare.

The destination can get messy, especially if many files and directories are moved. But it is still easily parsed manually or programmatically.

## What happens if a transfer is interrupted.

**Short answer:** Run it again and it'll be fine! You will lose the progress of active transfers but they will be fixed and it'll keep going.

Long answer: Incomplete versions may show up but will, by definition, a new version will be uploaded next time. It is also possible if an interruption is in the very short time between upload and saving in the database, that a complete version will be there and you upload again.

Note, *unlike* [rirb][rirb], there is *no need to refresh next time* because the files themselves define the state and the local cache is updated per-file.

## Why does some of the code look weird:

Simple answer: [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

I don't love it but it keeps the style consistent. One way to look at a compromise is each side is equally unhappy!

## I pruned older files. Why do I see those timestamps?

Files can stick around after pruning for a few reasons. The most common is that it hasn't been updated after the prune time so it needs to stick around.

Another possible scenario is that a file is moved and the referrer is not pruned. In that case, both the referent and the delete file must stay. The former because it is still needed and the latter so that it doesn't appear undeleted. This does add additional complication but it is considered in the prune logic.
