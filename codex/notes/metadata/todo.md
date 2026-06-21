# Metadata Restore TODO

Status: historical checklist. The authoritative current design is `codex/notes/metadata/design.md`. The v1 implementation was simplified after this checklist was written: it passes through nested rclone `Metadata`, synthesizes only stored source `mtime` as metadata key `mtime`, and does not implement the allowlist/denylist filtering described below.

## Purpose

This file is a handoff-oriented, detailed implementation checklist for the metadata restore feature designed in `codex/notes/metadata/design.md`.

It is intended to let a new session pick up the work without replaying the full conversation.

## High-Level Goal

Implement an opt-in restore feature that reuses source-origin metadata already present in the DB row and sends it in the main `rc.copyfile(...)` restore call via `_config.MetadataSet`, while optionally also reading live source metadata depending on `restore_metadata_source_live`.

This must preserve dfb's principle that auxiliary metadata is secondary.

## Agreed User Intent

These were explicitly agreed:

1. metadata should be set in the same restore copy call, not by a later fixup call
2. restore should use DB rows only, not inspect snapshot/log files directly
3. source-origin eligibility is `dstinfo == 0`
4. plain restore fallback with warning is preferred over hard failure when metadata context is missing or unusable
5. files only, not directories
6. stdout and `.rclonelink` skip metadata replay
7. keep code paths simple and unsurprising
8. least astonishment should win when choosing defaults or handling edge cases
9. document the feature carefully later in `docs/`
10. keep room for a future retry-without-metadata path, but do not implement it in v1

## Important Design Facts To Preserve

1. The feature is restore-specific and should initially live on `Restore`, not as a shared utility.
2. The main restore behavior remains one `rc.copyfile(...)` call per file.
3. `_config.MetadataSet` may be combined with `_config.Metadata` in that single call.
4. Restore-source backend feature `SlowModTime` is fetched once per restore session and cached.
5. `restore_metadata_source` is independent of global `config.metadata`.
6. `restore_metadata_source_live` defaults to `"auto"` and only suppresses live metadata in `auto` mode when replay is active and the restore copy source reports `SlowModTime`.
7. If no replayable keys remain after filtering, do not alter copy behavior for that file.

## Proposed File Touch List

This is the likely implementation surface.

1. `dfb/restore.py`
2. `dfb/configuration.py`
3. maybe `docs/CLI_help.md` if generated or manually maintained
4. one or more doc pages in `docs/` for the feature
5. `tests/test_backup_restore.py`
6. possibly `tests/test_units.py` if helper policy tests fit there better
7. `codex/TRANSCRIPT.md`

## Step-By-Step TODO

## 1. Re-read current restore path

Before editing:

1. inspect `dfb/restore.py`
2. confirm current transfer tuple shapes and restore worker flow
3. confirm current `.rclonelink` special-case logic
4. note current summary/logging behavior

Goal:
make the smallest possible change without disturbing the existing copy flow.

## 2. Add config entries

Add config support for:

1. `restore_metadata_source = False`
2. `restore_metadata_source_live = "auto"`

Requirements:

1. make the defaults visible in normal config handling
2. ensure existing `-o` override mechanism can set them
3. validate accepted values for `restore_metadata_source_live`:
   `auto`, `always`, `never`
4. do not add new dedicated CLI flags in v1

Also:

1. check where config docs/default examples live
2. keep any value-validation error messages simple and direct

## 3. Change restore transfer preparation to preload full rows

Current design choice:

1. still build the restore list normally
2. preload full DB rows by `rpath` before worker threads start
3. attach the row to each transfer entry

Implementation notes:

1. do not query DB inside concurrent worker threads
2. if a full row is missing for a transfer, warn and attach `None`
3. that file must still restore normally without metadata replay

Likely shape:

1. extend transfer entries from `(src, dst, size)` to something like `(src, dst, size, row)`
2. or use a dict/tuple variant if clearer, but keep worker logic simple

Important:

1. resolved references should use the resolved row already selected for restore
2. lookup key is `rpath`, not `(apath, timestamp)`

## 4. Cache restore-source features once per restore session

Add restore-session initialization logic to fetch restore-source backend features once.

Need:

1. obtain restore-source feature map from rc or existing helpers
2. store session-level decision or feature blob on `Restore`
3. expose the restore-source `SlowModTime` result to the metadata helper

Keep this simple:

1. no per-file feature calls
2. no extra destination metadata-capability probing beyond letting the copy call be authoritative

## 5. Add restore-local helper(s)

Add helper methods on `Restore` to keep worker code small.

Recommended helper split:

1. `_restore_live_metadata_enabled()` or equivalent session-level helper
2. `_metadata_candidates_from_row(row)` or equivalent filtering helper
3. `_restore_metadata_config(row, src, dst)` returning metadata-related `_config`

The final helper should decide:

1. whether replay is active for the file
2. whether `_config.Metadata` should be true or false
3. whether `_config.MetadataSet` should be present
4. any warning/debug logging for that file

Keep side effects minimal.

## 6. Implement eligibility checks exactly as designed

Replay must only happen when:

1. `restore_metadata_source` is true
2. destination is not stdout
3. source is not `.rclonelink`
4. full row is available
5. `dstinfo == 0`
6. at least one accepted key remains after filtering/validation

If any check fails:

1. fall back to plain restore behavior for that file
2. do not change live metadata behavior for that file

## 7. Implement metadata key filtering

Implement explicit allowlist filtering with hard-coded denylist.

### Accepted explicit keys

POSIX/system:

1. `mode`
2. `uid`
3. `gid`
4. `atime`
5. `mtime`
6. `btime`
7. `birthtime` compatibility alias only

Standard header-like:

1. `content-type`
2. `content-encoding`
3. `content-language`
4. `content-disposition`
5. `cache-control`
6. `expires`

Accepted prefixes:

1. `x-amz-meta-`
2. `x-goog-meta-`
3. `x-oss-meta-`
4. `metadata-`
5. `user-meta-`
6. `user.`

Rules:

1. matching is case-insensitive
2. emitted key is exactly as stored
3. `user.` is the only dotted-prefix exception
4. if both `btime` and `birthtime` exist, emit only `btime`

### Hard-coded denylist categories

Implement case-insensitive exact/prefix matching only.

Categories to exclude:

1. identity/versioning:
   `id`, `parent-id`, `version`, `version-id`
2. integrity/generated:
   `etag`, `md5`, `sha1`, `sha256`, `crc32`, `hash*`, `checksum*`
3. unstable ownership/display:
   `owner`, `owner-id`, `owner-display-name`, `group`, `group-name`
4. ACL/policy/security:
   `acl*`, `policy*`, `grant*`, `encryption*`, `kms*`, `sse*`
5. lifecycle/archive/status:
   `restore*`, `storage-class`, `tier`, `archive*`, `deleted*`, `is-*`
6. derived object facts:
   `size`, `content-length`

Also ensure structural dfb row keys never enter the candidate set.

## 8. Implement value handling

Rules to implement:

1. drop empty strings
2. drop null/`None`
3. stringify non-time scalar values
4. replay time-like keys only when already strings
5. skip numeric time values with warning
6. skip malformed values per-key, not per-file

Time-like keys:

1. `atime`
2. `mtime`
3. `btime`
4. `birthtime`

Important:

1. do not reinterpret or normalize time strings in v1
2. do not attempt recursive traversal of nested objects
3. operate on reconstructed top-level row keys only

## 9. Merge helper output into the main copy call

Modify restore worker logic so the existing `rc.copyfile(...)` call receives:

1. current copy config such as `NoCheckDest`
2. `_config.Metadata` according to live-policy resolution
3. `_config.MetadataSet` only when replay is active and non-empty

Preserve:

1. one main copy call per file
2. current restore error handling model
3. no post-copy metadata call
4. no automatic retry in v1

But:

1. structure the worker or helper so future retry-without-metadata can be inserted without rewriting everything
2. document that intention in code comments where appropriate, sparingly

## 10. Logging

Add run-level info logging at restore start:

1. whether manifest-backed metadata replay is enabled
2. configured `restore_metadata_source_live`
3. when mode is `auto`, what it resolved to for the session and why

Add debug logging:

1. per-file accepted key list when replay is active
2. optionally excluded metadata-looking keys for troubleshooting

Add warning logging:

1. missing full DB row for selected transfer
2. malformed/unusable values
3. all replay candidates dropped for a file

Do not add noisy warnings for every excluded key.

## 11. Tests: helper-level policy tests

Add unit-like tests for the new helper behavior. These should be the bulk of the coverage.

Cover at least:

1. replay disabled when `restore_metadata_source` is false
2. replay disabled when row is missing
3. replay disabled when `dstinfo != 0`
4. replay disabled for stdout
5. replay disabled for `.rclonelink`
6. `restore_metadata_source_live = "never"`
7. `restore_metadata_source_live = "always"`
8. `restore_metadata_source_live = "auto"` with restore-source `SlowModTime = True`
9. `restore_metadata_source_live = "auto"` with restore-source `SlowModTime = False`
10. explicit allowlist acceptance
11. denylist exclusion
12. `user.` acceptance and arbitrary dotted namespace rejection
13. `btime` preferred over `birthtime`
14. `birthtime` accepted only when `btime` absent
15. empty string dropped
16. null dropped
17. numeric time value skipped
18. non-time scalar stringified
19. all candidates dropped causes plain behavior

## 12. Tests: integration on local backend

The most important integration test was explicitly requested:

1. create source file with metadata,
2. back it up so the DB/snapshot captures the original metadata,
3. mutate the backed-up object metadata on `dst` after backup,
4. restore with `restore_metadata_source = True` and `restore_metadata_source_live = "always"`,
5. verify the restored file got the stored metadata, not the currently changed live metadata on the restore copy source.

This proves:

1. metadata replay happens in the single main copy call
2. `MetadataSet` overlay wins for intended keys even when live metadata remains enabled

Add at least one more integration test for a simpler branch:

1. missing-row fallback warning with successful content restore, or
2. skip behavior for stdout or `.rclonelink`

Use local backend because it has meaningful metadata behavior and is already exercised in metadata tests.

## 13. Check current metadata tests for reuse

Before adding new tests:

1. inspect existing metadata-related tests in `tests/test_backup_restore.py`
2. reuse helpers/patterns already used for local metadata verification
3. avoid duplicating expensive setup if a small adaptation of current metadata tests is enough

## 14. Documentation after code lands

The user explicitly requested eventual docs under `docs/`.

Create a dedicated metadata restore doc or section that explains:

1. why the feature exists
2. that it is DB-backed, not restore-time manifest parsing
3. config options and defaults
4. how `"auto"` uses restore-source `SlowModTime`
5. accepted key families
6. denied key categories
7. stdout and `.rclonelink` exclusions
8. warning-not-fatal behavior
9. one-call/no-retry v1 semantics
10. that auxiliary metadata remains secondary

Keep the docs explicit. The user specifically asked that filtering behavior be documented.

## 15. Update transcript

When implementation work begins or completes, add a one-line entry to `codex/TRANSCRIPT.md` in the existing style.

This turn also adds a transcript entry for the design/todo notes themselves.

## Things To Avoid

Do not accidentally expand scope into:

1. direct snapshot/log reading during restore
2. a generic metadata framework shared across unrelated commands
3. extra destination metadata-capability probing logic
4. regex-driven allow/deny rules
5. CLI-flag proliferation
6. directory metadata restore
7. post-copy metadata repair calls
8. automatic retry logic in v1
9. mutations to DB/snapshot state from restore behavior

## Sanity Check List Before Merging

Before considering the feature done:

1. verify plain restore behavior is unchanged when `restore_metadata_source = False`
2. verify `restore_metadata_source = True` still falls back cleanly when no row or no usable keys exist
3. verify `.rclonelink` and stdout skip metadata replay
4. verify `always` path uses one copy call and can still override changed source metadata
5. verify `auto` only disables live metadata when replay is active and restore-source `SlowModTime` is true
6. verify logs are useful but not noisy
7. verify docs reflect the exact implemented allowlist/denylist boundaries

## Open But Deferred

These are known future directions, not blockers:

1. retry without metadata after metadata-bearing copy failure
2. broader tools to regenerate/update/push manifests from DB
3. configurable namespace extensions
4. directory metadata support

If a new session needs context, start by reading:

1. `codex/notes/metadata/design.md`
2. this todo file
3. `dfb/restore.py`
4. existing metadata tests in `tests/test_backup_restore.py`
