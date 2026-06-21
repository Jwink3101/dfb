# (ADVANCED) Reading Backup and Prune `--dump` files

**ADVANCED**: This is **not needed** to understand the backup format. This is also useful to understand the *secondary* information.

For backups and prune, there is the option to `--dump` the line-delimited JSON(L) to a file. 

This can be used to manually upload files--either to an rclone destination or something different. This is almost certainly not a common use case but some details are included here.

This is subject to change...

There are four backup actions and one prune action represented in the JSON files

- Upload (new or modified file)
- Move by reference\*
- Move by server-side copy\*
- Delete
- Prune

Moves, noted with \*. also includes a delete.

The following are pulled from the test suite. Exact numbers may change depending on when it was run. The format is line-delimited JSON but has been pretty-printed (via `jq`) to be easier to understand. This also doesn't include metadata or hashes that may be attached (but do not affect the transfer)

**Note**: This is just an example. Depending on the situation, there could be additional keys.

## Upload

There is no distinction between new or modified files

```json
{
  "apath": "new.txt",
  "size": 4,
  "mtime": 1706224623,
  "rpath": "new.19700101000001.txt",
  "timestamp": 1,
  "dstinfo": false
}
```

This means to transfer (i.e. `copyto`) `src:<apath>` to `dst:<rpath>` or in this case, `src:new.txt` to `dst:new.19700101000001.txt`

## Path conversion rules (`apath` <-> `rpath`)

`dfb` stores file versions by inserting a timestamp tag (and optional flag) into the filename.

By default, the timestamp tag is the existing `datetag` format. If `filename_timestamp_format = "base36-epoch"` is configured, new writes instead use an 8-character fixed-width base36 UTC epoch token.

- Regular version: `name.ext` -> `name.YYYYMMDDhhmmss.ext`
- Delete marker: `name.ext` -> `name.YYYYMMDDhhmmssD.ext`
- Reference marker: `name.ext` -> `name.YYYYMMDDhhmmssR.ext`
- Regular version with `base36-epoch`: `name.ext` -> `name.00s7v4f1.ext`
- Delete marker with `base36-epoch`: `name.ext` -> `name.00s7v4f1D.ext`
- Reference marker with `base36-epoch`: `name.ext` -> `name.00s7v4f1R.ext`

The split between `name` and `.ext` is MIME-aware:

- First, split at the right-most dot.
- Then keep additional dotted suffixes (moving left) only while they are known MIME extensions.
- Example: `archive.tar.gz` is written as `archive.YYYYMMDDhhmmss.tar.gz`.

The reverse conversion (`rpath` to `apath`) is intentionally tolerant:

- It first tries the MIME-aware split.
- If that does not parse a supported timestamp tag, it falls back to a simpler "last-dot" parse.
- Readers accept both `datetag` and `base36-epoch`, so mixed histories remain readable.
- This allows both older and newer filename layouts to resolve to the same `apath`.

Known edge cases:

- Filenames manually created outside `dfb` can still be ambiguous when they mix date-like tokens and MIME-like suffixes.
- Which suffixes are treated as MIME extensions depends on Python's `mimetypes` registry in the running environment.
- This can change the *shape* of newly generated `rpath` names, but normal `dfb`-generated files are still expected to resolve back to the same `apath`.

Sometimes this could also have `isref` as False.

### Directory Marker

An empty directory marker has the same format
```json
{
  "apath": "my_empty_dir/.dfbempty",
  "mtime": -12345,
  "size": 0,
  "rpath": "my_empty_dir/.dfbempty.19700101000001",
  "timestamp": 1,
  "dstinfo": false
}
```
but has an `apath` with the filename `.dfbempty`. It also has an `mtime` of `-12345` but that is inconsequential.

While the format is the same and will result in the same *destination* file (`my_empty_dir/.dfbempty.19700101000001`), the filename of `.dfbempty` tells dfb to create an empty file with that `rpath` rather than copying it (since that file, presumably, does not exists in the source).

Note that these markers can be deleted all the same without any special handling.
 

## Move by reference

Moves also introduce a delete

```json
{
  "rpath": "move_by_ref.19700101000001.txt",
  "apath": "moved_by_ref.txt",
  "timestamp": 3,
  "size": 16,
  "mtime": 1706224623,
  "checksum": null,
  "isref": true,
  "ref_rpath": "moved_by_ref.19700101000003R.txt",
  "dstinfo": false,
  "original": "move_by_ref.txt"
}
```

The key fields to identify this `isref` is True (and `ref_rpath` and the value ended in `R`). This means to create a reference file at `dst:<ref_rpath>` that points to `<rpath>`. The most up-to-date format is:

`moved_by_ref.19700101000003R.txt`:

```json
{"ver": 2, "rel": "move_by_ref.19700101000001.txt"}
```

Note that the referent os *relative* to the referrer's parent directory. It can include `../` and/or subdirectories. 

**Note**: This *breaks* the usual relationship between `apath` and `rpath`

**Note**: Upon refresh, much of this information will be lost depending on whether the snapshot files are read

**Note**: If a reference file exists but its referent cannot be found during reference resolution (e.g. refresh, `advanced refresh-refs`, or `advanced lsjson-import` with reference refresh enabled), dfb logs a warning and keeps the item as deleted by setting `size = -1` for that entry.

## Move by copy

This looks a lot like the upload but is accomplished by a server-side copy. 

```json
{
  "rpath": "moved_by_copy.19700101000005.txt",
  "apath": "moved_by_copy.txt",
  "timestamp": 5,
  "size": 17,
  "mtime": 1706224623,
  "checksum": null,
  "isref": null,
  "ref_rpath": null,
  "dstinfo": false,
  "original": "move_by_copy.txt",
  "source_rpath": "move_by_copy.19700101000001.txt"
}
```
The key fields are `source_rpath`. This means to transfer (`copyto`)  `dst:<source_rpath>` to `dst:<rpath>` or in this case `dst:move_by_copy.19700101000001.txt` to `dst:moved_by_copy.19700101000005.txt`.

**Note**: When refreshed, some of the additional data may be lost depending on whether the snapshot files are read and it may not be possible to deduce this from a regular file. *That is the intended behavior*.

### Move by copy of a reference file

It is possible to move a referenced file. That will look like:

```json
{
  "rpath": "moved_by_ref_now_copy.19700101000005.txt",
  "apath": "moved_by_ref_now_copy.txt",
  "timestamp": 5,
  "size": 26,
  "mtime": 1706277100,
  "checksum": null,
  "isref": false,
  "ref_rpath": "moved_by_ref_then_copy.19700101000003R.txt",
  "dstinfo": false,
  "original": "moved_by_ref_then_copy.txt",
  "source_rpath": "move_by_ref_then_copy.19700101000001.txt"
}
```

Notice that `isref` is now false.

**The action is the same** as above. It just means you copy the *original* referent. And like above, a refresh may lose the additional information (which is expected) 

## Delete

Deletes are as follows:

```json
{
  "rpath": "delete.19700101000003D.txt",
  "apath": "delete.txt",
  "timestamp": 3,
  "size": -1,
  "mtime": 1706224623,
  "checksum": null,
  "isref": null,
  "ref_rpath": null,
  "dstinfo": false
}
```
The only thing that matters *is the existence of the file* `dst:<rpath>` or in this case `dst:delete.19700101000003D.txt`. However, dfb will write a 3-byte file with `DEL`. This is optional and doesn't make a difference.

## Prune

Prune entries are technically **optional** in the sense that upon refresh, they won't do anything since there will be nothing to note. But it is helpful if you wish to update the database and/or are importing

```json
{
  "_V": 1,
  "_action": "prune",
  "rpath": "delete.19700101000001.txt",
  "size": 25
}
```
This means to **delete** the file `dst:<rpath>` or `dst:delete.19700101000001.txt`. 

**Note**: If `rpath` is referenced, this will remove those entries from the DB too. See next note on how the file is removed.

**Note**: It is possible that the `rpath` is actually the path to a reference file. The entry *makes no distinction*. If applied to the reference file, nothing will happen in the DB but it symbolizes that the reference should have been removed (and presumably, so as not to break anything, the referent is also removed). 

## Comments

Entries with 

```json
{
  "_V": 1,
  "_action": "comment",
}
```
are ignore and any other key can be set. (In practice, it is just `"_action": "comment"` that makes it ignored but it is good practice to include the `"_V": 1,` in case this changes in the future)
