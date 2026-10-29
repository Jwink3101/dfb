2026-02-03 17:20 MST - Updated dfb-mount to use mfusepy and documented Linux/macOS install and usage; noted mfusepy requirement in readme.
2026-02-03 17:27 MST - Added quiet/verbose logging controls for dfb-mount and debug/info logging hooks; documented flags in mount docs.
2026-02-03 17:30 MST - Made --cache/--no-cache BooleanOptionalAction with default tied to --before/--at and documented caching behavior.
2026-02-03 17:45 MST - Added depth-limited empty-dir checks and deleted-file display option; documented flags in mount docs and readme.
2026-02-03 22:16 MST - Integrated mount as dfb subcommand with optional dfb[mount] extra, moved mount code under dfb/mount, and updated docs/readme/AGENTS.
2026-02-03 23:23 MST - Removed legacy dfb-mount wrapper and entrypoint; updated setup, docs, readme, and AGENTS.
2026-02-03 23:24 MST - Fixed dfb mount subcommand import wiring in dfb/cli.py.
2026-02-03 23:27 MST - Added --after time filter to dfb mount, matching dfb ls behavior, and documented it.
2026-02-03 23:30 MST - Updated mount docs to drop experimental label, reorder macOS FUSE-T instructions first, and clarify libfuse2/3 guidance.
2026-02-03 23:32 MST - Adjusted Linux mount install docs to prefer libfuse3 with libfuse2 fallback comments.
2026-02-03 23:44 MST - Silenced benign access permission probes in dfb mount to reduce mfusepy debug spam on macOS.
2026-02-09 00:17 MST - Removed deprecated link-helper package and script, and cleared related repo references.
2026-02-16 14:29 MST - Refined dfb mount empty-directory UX with grouped flags, boolean optional remove-empty switch, depth-failure behavior control, undated file handling modes, rclone mount arg rename, and added mount CLI/view tests.
2026-02-16 14:31 MST - Changed dfb mount --undated-files default to tag and updated docs/tests.
- 2026-02-23 23:24:34 -0700: Documented `apath`/`rpath` conversion rules and MIME-aware edge cases in `docs/adv_backup_dump_format.md`.
2026-02-24 13:29 MST - Migrated packaging metadata into pyproject.toml with dynamic dfb.__version__, removed setup.py, and deleted runtime git-version reporting.
2026-02-24 13:34 MST - Removed top-level dfb.py shadowing by adding dfb/__main__.py and updating build_help.py to run python -m dfb for CLI docs generation.
2026-02-24 14:25 -0700 - Added codex/ISSUES.md with detailed static-review findings for prune/restore/db grouping/fail-shell/RC lifecycle and async consistency gaps.
2026-02-24 15:02 -0700 - Added docs/format_spec.md draft defining stable object-format semantics, tolerant rpath parsing, auxiliary-log role, and apath stability invariants.
2026-02-24 15:06 -0700 - Added a design preamble to docs/format_spec.md emphasizing human-understandable, human-recoverable, and tool-optional format goals.
2026-02-24 15:12 -0700 - Added docs/aux_format_spec.md for optional logs/snapshots JSONL schema and precedence rules; linked it from docs/format_spec.md.
2026-03-02 13:45 MST - Added `advanced dbimport` support for `rclone lsjson` input files, including unresolved-reference resolution consistent with refresh, and added regression coverage.
2026-03-02 13:49 MST - Split rclone import into dedicated `advanced dbimport-lsjson`; restored `advanced dbimport` to dfb-export JSONL semantics to avoid format ambiguity.
2026-03-02 13:53 MST - Renamed the rclone importer command to `advanced lsjson-import`, clarified help text vs `dbimport`, and made its reference resolution always run the refresh-style post-pass.
2026-03-02 19:17 MST - Added `advanced refresh-refs` with `--use-snapshots/--no-use-snapshots`, reusing refresh reference-resolution methodology and adding regression coverage.
2026-03-02 19:19 MST - Added `--refresh-references/--no-refresh-references` controls to refresh, backup refresh, and lsjson-import; wired conditional reference resolution and added regression checks.
2026-03-02 19:26 MST - Added executable `dfb-cli.py` launcher script that imports `dfb.cli.cli` and calls it directly.
2026-03-02 19:28 MST - Documented unresolved-reference handling in `docs/adv_backup_dump_format.md` (warning + `size = -1` deleted fallback during reference resolution).
2026-03-04 12:49 MST - Tightened prune semantics: `--subdir` now filters prune targets to in-subdir `rpath`s only, `delete_rpath` now deletes all DB rows sharing an `rpath`, and added prune regressions for both behaviors.
2026-03-04 12:51 MST - Updated `codex/ISSUES.md` with current status of prior findings and added a new high-severity prune grouping issue for case-colliding `apath` names.
2026-03-04 12:53 MST - Clarified `codex/ISSUES.md` statuses and scope, corrected stale `group_by_apath` line references, and marked each numbered issue explicitly as open.
2026-04-21 17:01 MDT - Added `codex/notes/metadata/design.md` and `codex/notes/metadata/todo.md` capturing the agreed restore-metadata single-copy-call design, filtering rules, test strategy, and implementation handoff notes.
2026-04-21 17:01 MDT - Implemented opt-in DB-backed restore metadata replay in the main restore copy call, added local and helper tests, documented settings/limits in `docs/metadata_restore.md`, and clarified restore-source `SlowModTime` behavior in metadata notes.
2026-04-22 11:27 MDT - Added `codex/notes/remove-shell-script-todo.md` capturing the agreed hard-removal plan for restore `--shell-script`, including code/test/doc cleanup scope and a detailed checklist for continuing the work without chat history.
2026-04-22 12:01 MDT - Removed restore `--shell-script` support and the dead shell-script capture helper path, deleted dependent tests, scrubbed docs/internal notes, added a breaking changelog note, and updated the handoff TODO with verification results.
2026-04-29 14:48 MDT - Added `docs/base36_epoch_design.md` and updated `docs/format_spec.md` to specify optional `base36-epoch` filename timestamp encoding, dual-format parsing, ordering guarantees, and compatibility tradeoffs.
2026-04-29 15:03 MDT - Added `codex/notes/base36-epoch-implementation-plan.md` with a detailed implementation handoff covering config, parser/writer changes, call sites, and unit plus production-style test plans.
2026-04-29 16:05 MDT - Implemented optional `filename_timestamp_format = "base36-epoch"` across path parsing/writing, added `utils tsconv` and `utils apath2rpath --format`, expanded tests, updated docs/help/changelog, and checked off the base36 implementation plan.
2026-04-29 16:25 MDT - Added explicit parser-order unit coverage for base36/datetag fallback and split precedence, and extended refresh reference tests to run under both filename timestamp formats.
2026-05-01 12:32 MDT - Updated changelog and stale documentation for post-2025-07-11 features, expanded `build_help.py` command coverage to regenerate current CLI/doc indexes, and rebuilt `docs/CLI_help.md` plus generated readme doc lists.
2026-05-01 12:44 MDT - Updated `AGENTS.md` to reflect generated-doc workflow, optional mount install path, current docs/spec structure, and newer CLI smoke-check guidance.
2026-05-05 16:37 MDT - Tightened restore metadata row preloading to use selected `(apath, timestamp)` rows, added restore rclone version validation, corrected metadata restore docs, and added unit coverage for shared-rpath rows.
2026-05-05 16:49 MDT - Simplified restore metadata replay to pass through nested rclone `Metadata` and synthesize only stored source `mtime` as metadata `mtime`, updating design/docs and unit coverage.
2026-05-05 17:02 MDT - Verified rclone `MetadataSet["mtime"]` behavior and updated restore replay to force metadata mode when `MetadataSet` is active.
2026-05-05 17:10 MDT - Documented the V1 limitation that stored `mtime` replay uses rclone metadata mode rather than ordinary modtime repair, and added a V2 mtime assurance pathway.
2026-05-09 06:23 MDT - Investigated backup statistics undercounting moved-by-reference files; found reporting SQL excludes `isref` rows from logical size totals.
2026-05-09 06:30 MDT - Fixed backup current-size stats to include moved-by-reference files while preserving timestamp stored-byte reporting, and added regression coverage.
2026-06-06 16:58 MDT - Investigated prune 90-day vs 180-day dump counts; confirmed 180-day dump is a strict subset of 90-day dump under current cutoff semantics.
2026-06-07 09:15 MDT - Reviewed dfb backup object and auxiliary artifact semantics to characterize the backup model objectively.
2026-06-07 09:20 MDT - Reviewed current metadata/log/snapshot semantics and considered promoted metadata design options including sidecars.
2026-06-07 09:21 MDT - Explained current metadata recovery behavior and assessed whether secondary metadata is a fundamental design flaw.
2026-06-07 11:52 MDT - Documented the SQLite-specific bare-column MAX() behavior used by snapshot SQL selection.
2026-06-20 13:43 MDT - Fixed descriptor leaks around backup snapshot queries, SQLite helper contexts, prune DB lookups, and rclone RC HTTP responses that could surface as too-many-open-files during snapshot appends.
2026-06-20 13:43 MDT - Added an explicit Black target version of Python 3.13 to avoid formatting with a newer grammar than the local interpreter can parse.
2026-06-20 13:43 MDT - Fixed `DFBDST.init()` to close SQLite setup/probe connections after, not inside, transaction context-manager blocks.
2026-06-20 13:43 MDT - Added `DFBDST` regressions for reopening an existing DB and reusing the same SQLite connection across insert and snapshot operations.
2026-06-20 13:43 MDT - Made `test_rclonerc.py` independent of the current working directory so it can run from the repo-root pytest suite.
2026-06-20 13:43 MDT - Removed an unnecessary rclone RC test stderr logging handler that could emit after pytest closed captured streams.
2026-06-20 13:43 MDT - Reworked `DFBDST` to keep one shared SQLite connection per object and made tests explicitly reconnect long-lived helper handles after CLI commands.
2026-06-21 09:46 MDT - Incorporated the full post-5b34aa7 change set into the current `docs/changelog.md` NEXT entry.
2026-10-20 13:41 MDT - Versioned the backup object specification as 1.1.1, added retroactive 1.1.0/1.0.0 history, and clarified per-object timestamp and one-second precision semantics for continuous writers.
2026-10-28 16:51 MDT - Added per-thread rclone RC HTTP sessions, bounded connection-only retries with backoff, shutdown cleanup, and regression coverage for connection reuse, concurrent workers, and avoiding repeated submitted operations.
2026-10-28 16:58 MDT - Retained prune DB records on failed deletions with partial-failure reporting, added cancellation/join cleanup and source-error propagation to the thread mapper, and added prune/snapshot and subprocess hang regressions; 28 focused prune, worker, RC, backup, and restore tests passed with Black/isort checks.
2026-10-28 17:25 MDT - Updated the changelog and prepared the RC connection reuse, prune failure handling, and worker cleanup fixes for commit; retained the 28-test validation results.
