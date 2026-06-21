# Metadata Restore Design

## Purpose

This document captures the design agreed in conversation for a new restore feature that reuses source-origin metadata already stored in dfb's DB and auxiliary artifacts. The goal is to improve restore fidelity while keeping auxiliary manifests/logs secondary to the backup objects themselves.

This is a design document, not an implementation log.

## Problem Statement

dfb already stores additional file metadata in the DB, and snapshots/imports can enrich destination rows with source-origin metadata by flipping `dstinfo` back to false. Restore currently does not take advantage of that data beyond the ordinary rclone copy behavior.

The desired feature is:

1. If restore has source-origin metadata already available in the DB row, use that metadata during restore.
2. Do this in the main restore copy call, not with a post-copy metadata fixup call.
3. Optionally avoid reading live metadata from the restore copy source during restore when it is expensive to do so.
4. Keep auxiliary metadata secondary:
   core content restore must still work without it, and failures to apply metadata must not cause extra fallback complexity in v1.
5. Restore stored source `mtime` even when richer rclone metadata is unavailable.

## Non-Goals For V1

These were explicitly deferred or excluded:

1. No direct restore-time reading of snapshot/log files. Restore should use the DB row only.
2. No directory metadata restore.
3. No stdout metadata behavior.
4. No `.rclonelink` metadata replay.
5. No post-copy metadata replay call.
6. No destination metadata-capability probing beyond attempting the normal copy call.
7. Restore-source feature probing still occurs once per restore session for `SlowModTime` when evaluating `restore_metadata_source_live = "auto"`.
8. No automatic retry without metadata in v1, though code should be structured so that can be added later.
9. No changes to DB/snapshot state as a result of restore-time metadata replay.
10. No new dedicated CLI flags in v1; config options plus existing `-o` overrides are sufficient.

## Key Observations

### Current dfb behavior

1. Restore currently funnels through one `rc.copyfile(...)` call per file in `dfb/restore.py`.
2. Restore currently sets `_config["metadata"] = meta` based on `config.metadata`, except `.rclonelink` forces metadata off.
3. Snapshot/import enrichment already updates DB rows so they can become source-origin again:
   when snapshot data is applied in `dfb/dstdb.py`, rows are updated and `dstinfo` is reset to false.
4. Restore currently fetches only minimal row fields and does not preload full DB rows.

### Rclone behavior relevant to the design

These points were checked during design and matter to implementation:

1. RC accepts `_config.MetadataSet` as a dictionary.
2. The feature should remain a single `copyfile` call, potentially with both `_config.Metadata` and `_config.MetadataSet`.
3. Rclone metadata is string-key/string-value oriented.
4. `ModTime` is an rclone listing/stat field, while `mtime` is the rclone metadata key used in `MetadataSet`.
5. Standard time-like system metadata such as `mtime`, `atime`, and `btime` uses RFC 3339 strings.
6. Rclone only applies `MetadataSet` when metadata mode is enabled, so DB-backed replay must set `_config.Metadata = True`.
7. Backends vary in whether they honor specific metadata keys, so dfb should keep behavior simple and let the copy call outcome be authoritative.

## Config Surface

Two restore-specific config options are part of the design.

### `restore_metadata_source`

Type: `bool`

Default: `False` for v1

Meaning:
If true, restore may use source-origin metadata already present in the DB row and pass it to rclone via `_config.MetadataSet`.

This is independent of global `config.metadata`.

### `restore_metadata_source_live`

Type: enum string

Accepted values:

1. `"auto"`
2. `"always"`
3. `"never"`

Default: `"auto"`

Meaning:
Controls whether restore should also request live metadata from the restore copy source in the same `copyfile` call when manifest-backed metadata replay is active.

The intent is:

1. `"auto"`:
   if manifest-backed replay is active for a file, set `_config.Metadata = True` because rclone requires it for `MetadataSet`. If replay is not active, restore behaves as it does today.
2. `"always"`:
   keep `_config.Metadata = True` when the file is replayable, and also send `_config.MetadataSet`.
3. `"never"`:
   if replay is not active for the file, do not request live source metadata. If replay is active, set `_config.Metadata = True` because rclone requires it for `MetadataSet`.

If replay is not active for a given file, restore should behave as it does today.

## Eligibility Rules

Manifest-backed metadata replay is active for a file only when all of the following are true:

1. `restore_metadata_source` is true.
2. The restore target is a real file/object destination, not stdout.
3. The source object is not a `.rclonelink`.
4. The selected logical DB row was found successfully by `(apath, timestamp)`.
5. The row is source-origin, defined as `dstinfo == 0`.
6. The row has either a stored `mtime` value or a non-empty nested `Metadata` dictionary.

If any of these fail, restore should fall back to ordinary behavior for that file.

## DB Source Of Truth

The DB row is the only metadata source used during restore in v1.

That means:

1. Restore should not open or inspect snapshot/log files directly.
2. Preloaded DB rows are the place where source-origin metadata must already have been merged.
3. Missing full rows should generate a warning and fall back to plain restore for that file.

This keeps restore behavior tied to normalized DB state rather than ambient sidecar file availability.

## Restore Flow Design

### High-level flow

1. Build the restore transfer list as today.
2. Before concurrent copy workers start, preload the full DB row for each transfer by selected logical identity, `(apath, timestamp)`.
3. Cache restore-source backend features once per restore session.
4. For each transfer, call a restore-local helper that shapes metadata policy for that file.
5. Issue one `rc.copyfile(...)` call for the file.
6. The call may include `_config.Metadata`, `_config.MetadataSet`, both, or neither.

### Why preload rows

The agreed design intentionally avoids DB access from concurrent workers. Preloading rows:

1. keeps worker logic simple,
2. avoids DB-thread behavior questions,
3. still preserves the single main restore copy call per file.

### One call, one outcome

The restore worker should still make exactly one main rclone copy call per file.

The outcome of that call is authoritative in v1:

1. if rclone reports success, the file restore succeeded,
2. if rclone reports failure, the file restore failed,
3. no retry without metadata in v1,
4. implementation should still be structured so a future no-metadata retry can be inserted cleanly.

## Placement Of The New Logic

The metadata shaping logic should live on `Restore` in v1, not in a shared utility.

Suggested form:

1. one helper to preload full rows,
2. one helper to compute the restore-session live metadata decision from restore-source features,
3. one helper such as `_restore_metadata_config(row, src, dst)` that returns metadata-related copy config for that file.

This keeps the feature local to restore and avoids premature abstraction.

## Source Feature Caching

The restore-source `SlowModTime` decision must be cached once per restore session, not queried per file.

This is a backend capability and should not require repeated RC traffic during a large restore.

## Metadata Replay Rules

The guiding principle is that dfb should not become a general metadata translator.

The final v1 design is deliberately small:

1. Start with a shallow copy of the nested `row["Metadata"]` dictionary when it exists and is non-empty.
2. Independently synthesize `mtime` from top-level `row["mtime"]` when it exists.
3. The top-level stored `mtime` wins over `row["Metadata"]["mtime"]`.
4. Do not synthesize any other top-level metadata fields such as `mode`, `uid`, `gid`, `atime`, `btime`, content headers, ownership labels, or backend-specific fields.
5. Do not maintain a dfb allowlist or denylist for nested `Metadata` in v1.
6. Let rclone/backend acceptance of `MetadataSet` be authoritative.

### Nested `Metadata`

Nested `Metadata` is treated as rclone-produced metadata and passed through without dfb interpretation.

This avoids producer/consumer mismatch: rclone produced the keys and values, and rclone consumes them later.

If `row["Metadata"]` exists but is not a dictionary, skip it and continue with stored `mtime` if available.

### Stored `mtime`

Stored `mtime` is the one dfb-owned synthetic metadata key in v1.

Rules:

1. read top-level `row["mtime"]`
2. convert it from epoch seconds to RFC 3339 UTC
3. set `MetadataSet["mtime"]` with that value
4. never use `ModTime` as a `MetadataSet` key
5. if conversion fails, skip synthetic `mtime` and continue with nested `Metadata` if available

## Value Handling Rules

These rules are intentionally minimal.

### Nested metadata values

Nested `Metadata` values are passed through as stored. dfb does not normalize or validate nested metadata values in v1.

### Stored `mtime`

Stored `mtime` is converted to an RFC 3339 UTC string because dfb stores it as a numeric field while rclone consumes metadata `mtime` as a date string.

## Precedence Rules

If both live-source metadata and manifest-backed metadata are in play in the single call:

1. dfb only takes explicit responsibility for synthetic `mtime`
2. live-source metadata may still be requested via `_config.Metadata = True`
3. `MetadataSet` keys are intended to win for the specific keys dfb overlays
4. metadata not set by dfb remains under ordinary rclone/backend behavior when live metadata is enabled
5. rclone requires `_config.Metadata = True` for `MetadataSet`, so replaying DB-backed metadata can still require rclone metadata mode even when the live-source policy is `"never"`

This additive overlay model is considered the safest and least surprising v1 behavior.

## Logging Design

### Info-level logging

At restore start, log once:

1. whether `restore_metadata_source` is enabled,
2. what `restore_metadata_source_live` mode is configured,
3. when mode is `"auto"`, what live-source metadata behavior resolved for this restore session based on source features.

### Debug logging

At debug level:

1. log the metadata keys for each file when replay is active,
2. log skipped malformed source `mtime` values.

### Warning logging

Warn when:

1. the full DB row cannot be found for a selected transfer and plain restore is used,
2. metadata replay is requested but cannot be shaped for the file as expected.

Warnings should remain operationally meaningful and not explode for benign excluded keys.

## Special Cases

### Stdout restore

Metadata replay is skipped entirely for stdout restores because there is no destination object to annotate.

### `.rclonelink`

Metadata replay is skipped entirely for `.rclonelink` restores, matching the current spirit of special handling.

### Existing destination objects

Do not attempt to precompute whether metadata replay is moot because the destination may already exist. Let the single rclone copy call decide skip/transfer behavior.

## Behavior Relative To Core dfb Principles

This feature stays aligned with the agreed “auxiliary data is secondary” premise:

1. core content restore still works without auxiliary metadata,
2. missing or unusable metadata falls back to plain restore,
3. restore output may improve, but dfb history semantics still come from backup objects and DB interpretation,
4. no restore-time mutation of DB/snapshot truth is introduced.

## Testing Strategy

Testing should emphasize helper-level policy determinism and only a few end-to-end backend-sensitive cases.

### Unit tests

Most behavior should be tested at helper level:

1. eligibility rules,
2. live-source policy resolution,
3. pass-through of nested `Metadata`,
4. synthetic `mtime` conversion to metadata `mtime`,
5. stored `mtime` precedence over nested `Metadata["mtime"]`,
6. fallback behavior when neither nested `Metadata` nor stored `mtime` is usable.

### Integration tests

Add a small number of integration tests on a stable backend, especially local.

Most important integration test:

1. back up a file with metadata,
2. change source metadata after backup,
3. restore with `restore_metadata_source = True` and `restore_metadata_source_live = "always"`,
4. verify restored metadata matches DB-backed metadata rather than the changed current source metadata.

This proves the single-call overlay behavior, not merely a “source metadata disabled” path.

Additional integration behavior to cover:

1. plain fallback when DB row lookup fails,
2. skip behavior for stdout,
3. skip behavior for `.rclonelink`,
4. at least one policy test around `"auto"` and restore-source `SlowModTime`.

## Documentation Requirements

The user explicitly requested that this behavior be documented later in `docs/`.

When implemented, docs should explicitly cover:

1. feature purpose,
2. config options and defaults,
3. DB-only metadata source for restore,
4. the single-call design,
5. pass-through behavior for nested rclone `Metadata`,
6. synthetic `mtime` behavior from top-level stored `mtime`,
7. restore-source `SlowModTime` behavior in `"auto"` mode,
8. current v1 limitation that there is no automatic retry without metadata,
9. current v1 limitation that `mtime` replay uses rclone metadata mode, not ordinary `ModTime` repair.

## Future Extensions

These are intentionally left for later:

1. retry without metadata after a metadata-bearing copy failure,
2. direct restore-time reading of auxiliary manifests/logs,
3. configurable metadata filtering or mapping extension points,
4. broader auxiliary artifact tooling for regenerate/update/push workflows,
5. directory metadata restore,
6. mtime assurance for destinations that support ordinary modtime but not rclone metadata replay.

### Mtime assurance mode

The v1 implementation replays stored `mtime` through `MetadataSet["mtime"]`, which requires rclone metadata mode. That path is distinct from ordinary rclone `ModTime` handling.

A future mtime assurance mode could:

1. restore content first,
2. inspect or trust the selected row's stored `mtime`,
3. apply ordinary modtime through a rclone RC operation if available,
4. use `os.utime()` for local destinations when appropriate,
5. warn without failing content restore when no ordinary modtime path is available.

This should remain separate from general metadata replay because it may require a second post-copy operation.

## Implementation Summary

The smallest coherent implementation is:

1. add provisional config entries for `restore_metadata_source` and `restore_metadata_source_live`,
2. preload full DB rows for restore transfers by selected `(apath, timestamp)`,
3. cache source backend features once,
4. add a restore-local helper that returns metadata-related `_config` for a file,
5. merge the helper output into the existing single `rc.copyfile(...)` call,
6. add focused unit tests and a few local integration tests,
7. document the feature in `docs/`,
8. preserve room for a future retry-without-metadata path without implementing it yet.
