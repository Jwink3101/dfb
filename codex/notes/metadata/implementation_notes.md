# Metadata Restore Implementation Notes

## Purpose

This note captures implementation-specific context discovered while building the v1 restore metadata feature. It supplements `design.md` and `todo.md` with details that are easy to miss when returning in a new session.

## Important Context

### 1. Replayable metadata is often stored under `row["Metadata"]`

The restore DB row shape on local/rclone-backed paths is not just flat scalar keys.

In the tested row shape, replayable metadata was stored under a top-level nested dictionary:

```python
row["Metadata"] = {
    "atime": "...",
    "btime": "...",
    "gid": "...",
    "mode": "...",
    "mtime": "...",
    "uid": "...",
}
```

At the same time, the row also had scalar fields like:

1. `mtime` as a numeric epoch-like value
2. `dstinfo`
3. normal structural dfb row fields

The restore helper therefore needs to:

1. pass through the nested `Metadata` dictionary without dfb key/value translation,
2. synthesize only `mtime` from the top-level numeric `mtime` field,
3. let top-level stored `mtime` override `Metadata["mtime"]` because stored `mtime` must work even when richer rclone metadata is unavailable.

This matters because restore must work for sources that support ordinary mtime but do not support richer metadata. `MetadataSet` should use the rclone metadata key `mtime`, not the listing/stat field `ModTime`. Verification with rclone v1.74.0 showed that rclone only applies `MetadataSet["mtime"]` when metadata mode is also enabled.

This does not prove that `MetadataSet["mtime"]` works for destinations that support ordinary modtime but not rclone metadata replay. That case needs a future mtime assurance path if it becomes required.

### 2. Restore-time "live metadata" comes from the backup object on `dst`

For restore, the copy source is the backed-up object under `config.dst`, not the original configured source `config.src`.

That means:

1. `restore_metadata_source_live = "auto"` should probe `SlowModTime` on `config.dst`
2. integration tests that want to prove DB-backed metadata beats live metadata should mutate the backed-up object on `dst`, not the original file on `src`

### 3. Targeted integration tests need the workspace package, not the installed package

In this environment, plain `pytest` picked up the installed `dfb` package from site-packages for some test flows.

Use:

```bash
PYTHONPATH=/home/jwinokur/git_home/dfb pytest ...
```

for targeted verification so tests exercise the workspace code.

### 5. RC-based integration tests need socket access outside the sandbox

The RC-based restore path opens localhost sockets. In the sandboxed environment used here, that raised:

1. `PermissionError` from `socket.socket()` during `random_port()`

So end-to-end backup/restore tests that use RC may require escalated execution in this environment.

The focused commands that were used successfully were:

```bash
PYTHONPATH=/home/jwinokur/git_home/dfb pytest tests/test_units.py -k restore_metadata_config
PYTHONPATH=/home/jwinokur/git_home/dfb pytest tests/test_backup_restore.py -k 'test_metadata or test_restore_metadata_source'
```

The second command needed to run outside the sandbox so RC could bind localhost sockets.

### 6. `tests/testutils.py` needed a small robustness fix for symlinked `tests/testdirs`

In this workspace, `tests/testdirs` is a symlink. With the current Python version, using:

```python
self.pwd.parent.mkdir(exist_ok=True, parents=True)
```

could raise `FileExistsError` even with `exist_ok=True`.

The fix was to resolve the parent before `mkdir`:

```python
ignore.parent.resolve().mkdir(exist_ok=True, parents=True)
```

If similar setup issues appear later, check whether the path is symlinked before assuming a test regression in the metadata feature.
