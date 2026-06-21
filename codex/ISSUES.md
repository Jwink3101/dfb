# DFB Static Review Issues

Date: 2026-02-24
Last reviewed: 2026-03-04
Scope: Static code/design review plus targeted prune follow-up changes/tests

## Severity Legend

- High: Can cause silent data inconsistency, incorrect restore/prune behavior, or failure-path breakage.
- Medium: Reliability/API contract issue that can become operationally expensive.

## Status Update (2026-03-04)

- Addressed since original review (not originally tracked as numbered issues in this file):
  - Subdir prune no longer prunes outside-scope `rpath`s (filter added in `dfb/prune.py`).
  - `delete_rpath` now deletes all DB rows with matching `rpath` (shared-`rpath` reference model supported).
  - Regression tests added in `tests/test_prune.py` for both behaviors.
- Still open from this issue list:
  - #1, #2, #3, #4, #5, #6 below remain open at this time.

## 1) High - Prune can mark files deleted in DB even when remote deletion fails

Status: Open

### What this represents

The system-of-record metadata (`items` table and snapshots) can diverge from actual remote storage contents. This is a correctness issue, not just logging quality.

### Why this matters

- Users may believe data is pruned when it is still present remotely.
- Subsequent operations depending on DB state can behave inconsistently until a full refresh.
- This undermines trust in prune behavior and any reporting based on DB state.

### Code evidence

- `dfb/prune.py:164-174`: `_delete` catches exceptions but still returns `rpath`.
- `dfb/prune.py:176-184`: every returned `rpath` is fed to `self.dstdb.delete_rpath`, including failed deletes.
- `dfb/prune.py:170`: explicit message says it will "assume it is deleted."

### Operational symptoms

- Prune logs show deletion errors, but later `ls`/snapshot views no longer show those entries.
- Remote still contains the object.
- State appears to "self-correct" only after a refresh/relist.

### Minimal reproduction concept

1. Create a prune set where one object deletion fails (permission/transient backend error).
2. Run prune.
3. Observe DB/snapshot entry removed while object remains on remote.

### Recommended fix direction

- Return success/failure from `_delete` (for example `(rpath, ok)`), and only call `delete_rpath` when `ok` is true.
- Optionally keep current behavior only for specific "already missing" error classes after explicit error inspection.
- Track and report `failed_prunes` distinctly in summary output.

### Suggested tests

- Unit/integration test with mocked `rc.delete` failure verifies DB row is retained.
- Test for "already missing" remote response if intended to still remove DB row.

## 2) High - `restore-dir` empty result path never raises `SourceNotFoundError`

Status: Open

### What this represents

A control-flow bug causes expected error behavior to be skipped, turning "requested source not found" into a misleading no-op.

### Why this matters

- CLI semantics are inconsistent: users expect explicit "not found at time/path."
- Automation may interpret success/no-transfer as valid restore completion.
- This can hide wrong timestamp/path usage during incident recovery.

### Code evidence

- `dfb/restore.py:73-77`: builds generator `transfers` and consumes it with `list(transfers)`.
- `dfb/restore.py:78`: checks `if not transfers`, but generator truthiness is always true.
- As a result, `SourceNotFoundError` at `dfb/restore.py:79-81` is effectively unreachable in this path.

### Operational symptoms

- `restore-dir` on nonexistent/empty snapshot prints "No Transfers" instead of raising not-found.

### Minimal reproduction concept

1. Use `restore-dir` with a non-existent `--source-dir` or timestamp window with no matches.
2. Observe no exception and "No Transfers".

### Recommended fix direction

- Check `if not self.transfers:` after list materialization, not generator object truthiness.

### Suggested tests

- Restore-dir with empty snapshot result should raise `SourceNotFoundError`.
- Positive case should still proceed normally.

## 3) High - `group_by_apath` crashes on empty query result (affects prune and callers)

Status: Open

### What this represents

A low-level iterator helper assumes at least one row and throws `StopIteration` when result sets are empty. This is an unhandled edge case in normal operation.

### Why this matters

- Empty subdir/time-window prune should return cleanly, not crash.
- Makes behavior brittle for legitimate "nothing to do" scenarios.

### Code evidence

- `dfb/dstdb.py:1123`: `row = next(Qres)` without empty-result guard.
- Used by prune flow (`PruneableDFBDST.prune_rpaths`) where empty selections are valid.

### Operational symptoms

- Prune against empty/sparse scope can error unexpectedly instead of reporting "Nothing to prune."

### Minimal reproduction concept

1. Run prune in a subdir with no matching historical entries.
2. Observe potential unhandled `StopIteration`.

### Recommended fix direction

- Handle empty iterator in `group_by_apath`:
  - either early return generator with no yields, or
  - catch `StopIteration` and return.

### Suggested tests

- `group_by_apath` with no rows yields nothing and does not raise.
- Prune with no matches logs "Nothing to prune."

## 4) High - backup failure hook (`fail_shell`) has broken variable references

Status: Open

### What this represents

Error-handling path intended for incident response is itself broken. Fail-path observability and notification hooks may not run with expected context.

### Why this matters

- The most important moment for diagnostics (backup failure) loses reliable notification context.
- Broad exception suppression masks this defect and makes troubleshooting harder.

### Code evidence

- `dfb/cli.py:1103-1106` references non-existent names/attributes:
  - `log.log_file`
  - `log.debug_file`
  - `config._configpath` (actual attribute in Config is `configpath`)
- Wrapped in broad `except` at `dfb/cli.py:1118-1119`, so failure is hidden behind generic log message.

### Operational symptoms

- On backup exception, `fail_shell` may not run correctly or receives wrong env vars.
- User sees generic "Saving logs and running fail_shell didn't work" with limited detail.

### Recommended fix direction

- Replace with valid values:
  - `LOGPATH`: `str(config.logfile.resolve())`
  - `DEBUGPATH`: `str(config.debuglogfile.resolve())`
  - `CONFIGDIR`: `str(config.configpath.parent)`
- Narrow exception handling and log traceback/details in debug mode.

### Suggested tests

- Force backup exception and assert `fail_shell` invoked with expected env.
- Verify non-zero shell return obeys `stop_on_shell_error` behavior in failure paths.

## 5) Medium - RC wrapper has state/async contract inconsistencies

Status: Open

### What this represents

The RC abstraction advertises certain behavior (`use_async`, restartability) but implementation is inconsistent in a few methods.

### Why this matters

- Can lead to surprising behavior in long-lived processes or future refactors relying on `use_async`.
- Inconsistent contracts increase maintenance risk and debugging time.

### Code evidence

- `dfb/rclonerc.py:171-190`: `stop()` does not set `_started = False`.
  - Subsequent `start()` may short-circuit incorrectly if object reused.
- `dfb/rclonerc.py:219-233`: `write()` computes `method` but calls `self.call(...)` directly.
- `dfb/rclonerc.py:573-575`: `stat()` computes `method` but calls `self.call(...)` directly.

### Operational symptoms

- Potential inability to cleanly restart a stopped RC instance in same process.
- `use_async=True` ignored in `write()`/`stat()`.

### Recommended fix direction

- Set `_started = False` in `stop()` after process termination path.
- Use `method(...)` in `write()` and `stat()` to honor `use_async`.
- Consider adding explicit lifecycle state checks and tests for start-stop-start.

### Suggested tests

- Start/stop/start on same RC object should work.
- `use_async=True` paths should invoke async flow consistently.

## 6) High - Prune grouping can mis-group case-colliding `apath`s

Status: Open

### What this represents

`group_by_apath` orders by `LOWER(apath), timestamp` but groups by exact `apath` equality. If case-distinct names exist (for example `A.txt` and `a.txt`), rows can interleave and split one logical group into multiple chunks.

### Why this matters

- Prune cutoff and delete-marker handling depend on complete per-`apath` history ordering.
- Split/interleaved groups can produce incorrect prune sets for affected files.
- Behavior becomes backend/filesystem dependent (case-sensitive vs case-insensitive naming practices).

### Code evidence

- `dfb/dstdb.py:1117-1118`: ordering uses `LOWER(apath), timestamp`.
- `dfb/dstdb.py:1130-1136`: grouping uses exact `row["apath"] == name`.

### Operational symptoms

- In repositories containing both `A.txt` and `a.txt`, prune may keep/delete unexpected versions.

### Recommended fix direction

- Make ordering and grouping semantics consistent:
  - either order by exact `apath, timestamp`, or
  - explicitly normalize both ordering and grouping to the same key with a documented policy.

### Suggested tests

- Add prune fixture with case-colliding paths and verify stable prune results under intended policy.

## Additional design note (intentional but high-risk by model)

Config and overrides are executed with `exec` by design. This provides flexibility but is a major trust boundary and code-execution surface. This is documented in project docs and comments; not treated as a bug in this review, but should remain prominently documented for operators.
