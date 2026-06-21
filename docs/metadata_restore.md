# Metadata Restore

`dfb` can optionally reuse source-origin metadata already stored in its DB when restoring files.

This is meant to improve restore fidelity while keeping auxiliary metadata secondary. The backup objects still define the core backup truth.

## Summary

When enabled, restore can pass stored metadata to rclone in the main restore copy call using rclone metadata replay support.

This is useful when:

1. the backed-up object no longer has all the metadata you care about,
2. the restore source backend is expensive or slow to query for metadata,
3. you want restored file metadata to follow the metadata captured at backup time rather than whatever metadata the backup object happens to have now.

## Settings

Add these settings to your config if you want to enable the feature.

```python
restore_metadata_source = True
restore_metadata_source_live = "auto"  # "auto", "always", "never"
```

Defaults:

```python
restore_metadata_source = False
restore_metadata_source_live = "auto"
```

## How It Works

Restore loads the selected file rows from the dfb DB and, for files whose row metadata is source-origin (`dstinfo == 0`), it may pass stored metadata through rclone's metadata replay mechanism in the same restore copy call.

Important details:

1. Restore uses the DB row only. It does not read snapshot or log files directly during restore.
2. The metadata replay happens in the main restore copy call, not as a second post-copy metadata fixup step.
3. The restore copy source is the backed-up object on `dst`, so any "live source metadata" during restore refers to metadata read from the backup object being restored.
4. If the DB row is missing or has no replayable `Metadata` or `mtime`, restore falls back to ordinary restore behavior for that file.
5. If multiple logical rows share the same backup object, such as with reference files, restore replays metadata from the selected logical row while still reading content from the shared backup object.

## Live Source Metadata Policy

`restore_metadata_source_live` controls whether restore also asks rclone to read metadata from the restore copy source in the same call.

Important: rclone only applies `MetadataSet` when metadata mode is enabled. When DB-backed metadata replay is active for a file, dfb enables rclone metadata mode even if `restore_metadata_source_live` would otherwise disable live metadata.

### `"auto"`

If DB-backed metadata replay is not active for a file, ordinary restore metadata behavior applies. If replay is active, dfb enables rclone metadata mode because rclone requires it for `MetadataSet`.

The `SlowModTime` probe is still logged for visibility, but it cannot disable metadata mode for a file that is replaying `MetadataSet`.

### `"always"`

Keep live source metadata enabled when replay is active and also send DB-backed metadata in the same restore copy call.

This is the strongest overlay mode and is useful when you want DB-backed metadata to win for the keys dfb explicitly replays.

### `"never"`

If replay is not active for a file, do not ask rclone to read live source metadata for that file. If replay is active, dfb still enables rclone metadata mode because rclone requires it for `MetadataSet`.

## What Metadata Is Replayed

The v1 implementation intentionally keeps dfb out of general metadata translation.

Restore builds `MetadataSet` from:

1. the nested rclone-produced `Metadata` dictionary already stored in the selected DB row, when present,
2. top-level stored `mtime`, converted from epoch seconds to the rclone metadata key `mtime`.

Stored top-level `mtime` wins over `Metadata["mtime"]`. dfb does not synthesize any other top-level metadata fields such as `mode`, `uid`, `gid`, `atime`, `btime`, content headers, ownership labels, or backend-specific fields.

`ModTime` is an rclone listing/stat field. Restore uses `mtime` as the `MetadataSet` key.

## What Is Not Replayed

The v1 implementation does not maintain a dfb allowlist or denylist for nested `Metadata`. Nested metadata is passed through as stored and rclone/backend acceptance is authoritative.

Structural dfb DB fields such as `rpath`, `apath`, `timestamp`, `size`, `dstinfo`, and similar are not replayed, except that top-level `mtime` is intentionally converted to metadata `mtime`.

## Value Rules

Nested `Metadata` values are passed through as stored. dfb does not normalize or validate them in v1.

Top-level stored `mtime` is converted to an RFC 3339 UTC string. If conversion fails, dfb skips synthetic `mtime` and still replays nested `Metadata` if available.

## Fallback Behavior

Auxiliary metadata remains secondary.

That means:

1. if a DB row cannot be loaded, restore still restores the file contents and warns,
2. if stored `mtime` cannot be converted, restore still restores the file contents and logs debug details when enabled,
3. if rclone rejects the single metadata-bearing copy call, restore treats that file as failed for now,
4. v1 does not automatically retry the restore without metadata.

The code is structured so a future no-metadata retry path can be added later if wanted.

## Logging

When enabled, restore logs one debug-level line describing:

1. whether DB-backed metadata replay is enabled,
2. the configured live-source policy,
3. for `"auto"`, whether live source metadata will be disabled or kept when replay is active based on `SlowModTime`.

Per-file metadata key lists are also debug-level.

## Current Limitations

The v1 implementation intentionally does not do the following:

1. no restore-time direct reads of snapshot/log sidecar files,
2. no directory metadata replay,
3. no metadata replay for stdout restores,
4. no metadata replay for `.rclonelink` restores,
5. no automatic retry without metadata after a metadata-bearing copy failure,
6. no post-copy ordinary modtime repair.

Stored `mtime` is replayed through rclone metadata mode as `MetadataSet["mtime"]`. This has been verified for local restores where rclone metadata mode supports `mtime`, but it is not the same path as ordinary rclone `ModTime` handling. A restore destination may support ordinary modtime while not supporting rclone metadata replay; v1 does not add a second step to set modtime through another mechanism in that case.

## Future Mtime Assurance

A future version may add an mtime assurance mode for destinations that support ordinary modtime but not rclone metadata replay.

That mode would restore file contents first, then verify or apply the selected row's stored `mtime` through the best available ordinary modtime path. Possible implementations include a rclone RC modtime operation if one is available for the backend, direct `os.utime()` for local destinations, or a warning-only fallback when neither path works.

This would intentionally be separate from general metadata replay because it may require a second post-copy operation.

## Relationship To Snapshots And Logs

This feature uses metadata already merged into the DB row. Snapshot/log files remain secondary support artifacts.

If you refresh or import data in ways that enrich the DB with source-origin metadata, restore can benefit from that. But restore does not require those artifacts to exist at restore time.
