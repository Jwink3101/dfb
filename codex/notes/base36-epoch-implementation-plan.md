# Base36-Epoch Implementation Plan

## Purpose

This file captures a concrete implementation plan for adding the optional `base36-epoch` filename timestamp format to `dfb`.

The goal is that another session can pick this up and implement it without relying on chat history.

This is an implementation handoff document, not the normative format spec. The design and format contract are in:

- `docs/base36_epoch_design.md`
- `docs/format_spec.md`

## Progress Checklist

- [x] Add format primitives in `dfb/utils.py`.
- [x] Add config default and validation.
- [x] Update `apath2rpath()` writing behavior.
- [x] Update `rpath2apath()` reading behavior.
- [x] Update CLI utilities and user-visible path conversion helpers.
- [x] Review mount view and path-parsing consumers; no code change needed.
- [x] Add focused unit tests for encoding/parsing behavior.
- [x] Add CLI and higher-level tests.
- [x] Update docs/help/config guidance.
- [x] Run focused verification.

## Agreed Design Summary

These points were explicitly agreed:

1. `datetag` remains the default and primary canonical filename timestamp format.
2. Add `base36-epoch` as a formal optional format for path-length-sensitive backups.
3. `base36-epoch` is:
   - unsigned UTC epoch seconds
   - fixed width 8
   - lowercase canonical write form
   - alphabet `0123456789abcdefghijklmnopqrstuvwxyz`
   - parsed case-insensitively
   - lexicographically sorted within the format
4. Writers use the configured preferred format.
5. Readers always support both formats.
6. Parse order is:
   - MIME-aware split first
   - fallback plain last-dot split second
   - within each split, preferred format first, alternate second
7. Mixed-format histories are valid.
8. Cross-format lexical sort is not guaranteed.
9. Canonical flags remain `D` and `R`.
10. Readers should accept lowercase `d` and `r` as a compatibility expansion.
11. Forward compatibility with old `dfb` is not required.
12. The exact config variable name was intentionally left open in the design, but the value names are fixed as `datetag` and `base36-epoch`.

## Current Code Surfaces

These are the expected primary touch points.

### Core path conversion

- `dfb/utils.py`
  - `time2all`
  - `rpath2apath`
  - `parse_dateflag`
  - `apath2rpath`
  - `re_datetag`
  - `smart_splitext`

### Config loading and validation

- `dfb/configuration.py`
  - template defaults
  - `_validate()`
  - config enum validation set

### User-facing helpers and consumers

- `dfb/cliutils.py`
- `dfb/mount/view.py`
- `dfb/dstdb.py`
- `dfb/backup.py`
- `dfb/__init__.py`

### Tests

- `tests/test_units.py`
- `tests/test_cli_utils.py`
- `tests/test_mount_view.py`
- `tests/test_mount_cli.py`
- `tests/test_backup_restore.py`
- `tests/test_listing.py`

### Docs

- `docs/format_spec.md`
- `docs/base36_epoch_design.md`
- `docs/adv_backup_dump_format.md`
- `docs/CLI_help.md`
- maybe `docs/mount.md`
- maybe `docs/readme.md`
- maybe `docs/changelog.md`

## Implementation Strategy

Recommended implementation order:

1. Add format primitives in `dfb/utils.py`.
2. Add config default and validation.
3. Update `apath2rpath()` writing behavior.
4. Update `rpath2apath()` reading behavior.
5. Update CLI utilities and any user-visible path conversion helpers.
6. Update mount view and any path-parsing consumers if needed.
7. Add unit tests first.
8. Add CLI and higher-level tests.
9. Update docs/help snapshots.
10. Run focused verification.

This feature is deceptively localized. Most behavior flows through `apath2rpath()` and `rpath2apath()`, but many commands and tests depend on their exact behavior.

## Proposed Config Surface

### Config name

The design intentionally did not freeze the exact config key name, but the implementation should choose one clear name and use it consistently.

Recommended name:

- `filename_timestamp_format`

Recommended allowed values:

- `"datetag"`
- `"base36-epoch"`

Recommended default:

- `"datetag"`

Reason:

- explicit
- future-extensible
- clear in config dumps and docs

### Validation

Add the setting to the enum validation set in `Config._validate()`.

The pattern already exists for fields like `compare`, `dst_compare`, and `restore_metadata_source_live`.

Expected validation behavior:

1. accept `"datetag"`
2. accept `"base36-epoch"`
3. reject anything else with a config error

## Core Code Changes

### 1. Add new encode/decode helpers in `dfb/utils.py`

Add explicit helpers rather than burying the logic inside `parse_dateflag()` or `apath2rpath()`.

Recommended helpers:

1. `encode_base36_epoch(ts)`
2. `decode_base36_epoch(token)`
3. `parse_timestamp_tag(tag, *, preferred="datetag")`
4. optionally `format_timestamp_tag(ts, *, fmt="datetag")`

Recommended behavior:

- `encode_base36_epoch(ts)`
  - input: integer epoch seconds
  - reject negative values
  - reject values above `36**8 - 1`
  - emit lowercase 8-character token with left zero-padding

- `decode_base36_epoch(token)`
  - require exactly 8 characters
  - case-fold to lowercase
  - accept only `0-9a-z`
  - return integer epoch seconds

- `format_timestamp_tag(ts, fmt=...)`
  - for `datetag`, use existing `time2all(ts).dt`
  - for `base36-epoch`, use `encode_base36_epoch(...)`

- `parse_timestamp_tag(tag, preferred=...)`
  - strip leading `.`
  - try preferred format
  - then alternate format
  - return `(ts, flag, matched_format)`
  - raise `ValueError` if both fail

Do not overload `time2all()` to emit base36. `time2all()` is currently a general “convert to all common timestamp forms” helper and remains useful as-is.

### 2. Refactor `parse_dateflag()`

Current `parse_dateflag()` is datetag-specific.

Options:

1. keep `parse_dateflag()` as datetag-only and add a new general parser
2. widen `parse_dateflag()` to parse both formats

Recommended approach:

- keep a datetag-specific helper for clarity
- add a new generic helper and move callers over to it

Recommended end state:

1. `parse_datetag_flag(...)` for the old exact regex semantics
2. `parse_timestamp_tag(...)` for both formats

This will keep the code easier to reason about and simpler to test.

### 3. Update `apath2rpath()`

Current behavior:

- `ts, dt, _, _ = time2all(ts)`
- write `f"{base}.{dt}{flag}{ext}"`

Required change:

- choose timestamp tag based on config preference

Important dependency:

`apath2rpath()` currently does not take a config object and is imported widely.

There are two implementation paths:

#### Option A: add a keyword argument

Example:

- `apath2rpath(apath, ts=None, *, flag="", verify=True, fmt="datetag")`

Pros:

- explicit
- testable
- no hidden global state

Cons:

- requires updating every write-side caller that should honor config

#### Option B: preserve old API and add a config-driven wrapper elsewhere

Pros:

- less immediate churn in callers

Cons:

- risks split write behavior across the codebase
- easier to miss a call site

Recommended approach:

- Option A

Suggested signature:

```python
def apath2rpath(apath, ts=None, *, flag="", verify=True, fmt="datetag"):
```

Then update all internal write-side call sites to pass `fmt=config.filename_timestamp_format`.

Expected caller updates:

- `dfb/backup.py`
- any future writer helper code
- `dfb/cliutils.py` for `utils apath2rpath`

Important:

- keep the default `fmt="datetag"` so old internal/test callers do not break unnecessarily
- keep `verify=True` semantics
- sanity-check round trip against the generic parser

### 4. Update `rpath2apath()`

Current behavior:

1. MIME-aware split via `smart_splitext()`, then `os.path.splitext(...)`
2. fallback `os.path.splitext(rname)`
3. both use `parse_dateflag()`

Required change:

- support both `datetag` and `base36-epoch`
- allow preferred-first parse order

Recommended signature:

```python
def rpath2apath(rpath, *, preferred="datetag"):
```

Behavior:

1. MIME-aware split
2. try preferred format
3. try alternate format
4. fallback last-dot split
5. try preferred format
6. try alternate format
7. raise `NoTimestampInNameError` if all fail

Important:

- do not add arbitrary scanning through earlier dot tokens
- preserve current split behavior exactly
- preserve current return tuple shape `(apath, ts, flag)`

### 5. Flag parsing

Current behavior:

- uppercase `D`/`R` only

Required change:

- read `d`/`r` too
- canonicalize to uppercase in returned `flag`

Recommended rule:

- output from parsers should always be `""`, `"D"`, or `"R"`

This keeps higher-level code unchanged.

### 6. Keep `datetag`-looking ambiguity unchanged

The design explicitly accepts bounded ambiguity similar to the current date-like slot behavior.

Implementation rule:

- if the candidate token in the timestamp slot matches the preferred format, take it
- only fall back to the alternate if preferred fails

This matters most for future mixed histories and manual filenames.

### 7. Update `dfb/backup.py`

Current write sites:

- regular objects
- delete markers
- reference markers

Update all calls to `apath2rpath(...)` so they pass the configured format.

Likely sites:

- normal backup write
- delete marker write
- move/reference paths

The key requirement is consistency: all new object writes during a backup run must use the same configured format.

### 8. Update `dfb/cliutils.py`

For `utils apath2rpath`:

- use configured preferred format if config is loaded
- or decide explicitly whether the standalone utility should stay datetag-by-default unless config says otherwise

This needs one implementation decision:

1. should `dfb utils apath2rpath` reflect config if `--config` is provided
2. should it remain purely datetag unless extended with a dedicated option

Recommended implementation:

- if invoked in the normal config-aware CLI context, use the config value
- otherwise default to `datetag`

For `utils rpath2apath`:

- allow both formats
- use config preference only for preferred parse order

### 8a. Add a timestamp-tag conversion utility

The current split commands:

- `dfb utils apath2rpath`
- `dfb utils rpath2apath`

are not enough for direct timestamp-tag conversion and format exploration.

Recommended addition:

- `dfb utils tsconv`

Recommended behavior:

1. accept any supported timestamp representation that `dfb` can already interpret for this purpose:
   - `YYYYMMDDhhmmss`
   - `base36-epoch` 8-character token
   - optionally other timestamp forms already accepted by `timestamp_parser`, if implementation wants the tool to be more general
2. expose explicit output-format control
3. support one or many inputs
4. emit structured JSON lines or a simple raw mode

Recommended CLI shape:

```text
dfb utils tsconv --to datetag values...
dfb utils tsconv --to base36-epoch values...
```

Example:

```text
dfb utils tsconv --to base36-epoch 20260425102354
dfb utils tsconv --to datetag 00s7v4f1
```

Recommended default JSON output:

```json
{"input":"20260425102354","timestamp":"2026-04-25T10:23:54+00:00","datetag":"20260425102354","base36_epoch":"01234567"}
```

Recommended optional raw output:

```text
01234567
```

Why this name:

1. short
2. directly names the actual object being converted
3. does not imply file-path conversion

Implementation recommendation:

1. parse the input token into integer epoch seconds
2. render both canonical forms from that integer
3. let `--to` control which field is emitted in raw mode
4. internally reuse the same encode/decode helpers used by `apath2rpath()` and `rpath2apath()`

Test coverage to add for this utility:

1. datetag input to base36 output
2. base36 input to datetag output
3. uppercase base36 input accepted
4. multiple input values
5. malformed datetag rejected
6. malformed base36 rejected
7. optional raw-output mode

### 8b. Extend `utils apath2rpath`

`apath2rpath` should become more flexible as part of this feature.

Recommended behavior:

1. accept `--date` in either canonical filename timestamp form:
   - `YYYYMMDDhhmmss`
   - 8-character `base36-epoch`
2. continue accepting broader timestamp forms already supported by `timestamp_parser` unless that creates implementation conflicts
3. add explicit output-format selection for the generated `rpath`

Recommended CLI shape:

```text
dfb utils apath2rpath --date 20260425102354 --format datetag files...
dfb utils apath2rpath --date 01234567 --format base36-epoch files...
```

Recommended rules:

1. if `--format` is specified, it overrides config for this utility invocation
2. if `--format` is omitted, use configured `filename_timestamp_format`
3. `--date` parsing should accept either timestamp token format regardless of output format

This is useful because the user may want:

1. to preserve a known moment in time while changing only the filename encoding
2. to verify cross-format equivalence directly from the CLI

Test coverage to add:

1. `--date` with datetag input and datetag output
2. `--date` with datetag input and base36 output
3. `--date` with base36 input and datetag output
4. `--date` with base36 input and base36 output
5. `--format` overriding config
6. malformed `--date` token rejection

### 9. Update mount consumer behavior

`dfb/mount/view.py` uses `rpath2apath()` in `_safe_rpath2apath()`.

Because it is a reader, it should always accept both formats.

Implementation choices:

1. keep default preferred `datetag`
2. thread config-preferred order into mount as well

Recommended:

- keep mount reader tolerant regardless
- if config is readily available, pass preferred format through
- if not, default preferred to `datetag`

The important part is that mount must not regress on mixed histories.

### 10. Update docs/help/config template

Required doc surfaces:

- config template comments in `dfb/configuration.py`
- `docs/CLI_help.md` if config help snapshots surface the new setting
- `docs/adv_backup_dump_format.md`
- maybe `docs/readme.md`
- `docs/changelog.md`

The docs added earlier in this session are design/spec docs, not end-user config guidance.

## Detailed Test Plan

The tests should be split into two classes:

1. small focused unit tests for encoding/parsing behavior
2. production-style tests that cover real command and data flows

### A. Small unit tests

Primary file:

- `tests/test_units.py`

Add targeted tests rather than only expanding the existing broad loops.

#### A1. Base36 encode tests

Add direct tests for `encode_base36_epoch()`:

1. `0 -> "00000000"`
2. `1 -> "00000001"`
3. known value `1706262301 -> "00s7v4f1"`
4. `36**8 - 1 -> "zzzzzzzz"`

Negative tests:

1. negative epoch raises
2. `36**8` raises

#### A2. Base36 decode tests

Add direct tests for `decode_base36_epoch()`:

1. `"00000000" -> 0`
2. `"00000001" -> 1`
3. `"00s7v4f1" -> 1706262301`
4. `"00S7V4F1" -> 1706262301`
5. `"zzzzzzzz" -> 36**8 - 1`

Negative tests:

1. wrong length short
2. wrong length long
3. invalid character like `_`
4. invalid character like `-`

#### A3. Datetag parser regression tests

Keep explicit tests proving no regressions:

1. existing datetag still parses
2. existing uppercase `D` and `R` still parse
3. lowercase `d` and `r` parse and canonicalize to uppercase

#### A4. `rpath2apath()` base36 tests

Add direct `rpath2apath()` cases for:

1. `file.00000000.txt -> ("file.txt", 0, "")`
2. `file.00000001.txt -> ("file.txt", 1, "")`
3. `file.00s7v4f1.txt -> ("file.txt", 1706262301, "")`
4. `file.00s7v4f1D.txt -> ("file.txt", 1706262301, "D")`
5. `file.00s7v4f1R.txt -> ("file.txt", 1706262301, "R")`
6. lowercase flags `d` and `r`
7. no extension
8. dotfile case like `.dfbempty.00000001`
9. multi-extension case like `archive.00s7v4f1.tar.gz`

#### A5. `apath2rpath()` format-selection tests

Add coverage that the write helper emits the right format:

1. `fmt="datetag"` uses 14-digit tag
2. `fmt="base36-epoch"` uses 8-char tag
3. flags are appended after the timestamp token
4. verify round-trip still succeeds

#### A6. Ambiguity/order tests

Add tests that exercise preferred-first behavior.

These do not need to be exotic. The goal is to pin the decision order.

Recommended cases:

1. a plain datetag filename still parses the same with either preferred format
2. a plain base36 filename still parses the same with either preferred format
3. a manually chosen token that could plausibly be text should only be interpreted when it is in the timestamp slot
4. MIME-aware split is still attempted before fallback split

#### A7. Existing edge-case regression tests

Extend the current `test_rpath2apath()` and `test_apath2rpath()` cases rather than replacing them.

Keep current special cases:

1. old placement compatibility
2. double-date names
3. MIME-aware ambiguous cases
4. `verify=True` fallback behavior

Add equivalent base36 cases for:

1. multiple timestamp-like tokens
2. MIME-aware multi-extension names
3. hidden files

### B. CLI utility tests

Primary file:

- `tests/test_cli_utils.py`

#### B1. `utils apath2rpath`

Add cases that verify:

1. default config still emits datetag
2. config override to `base36-epoch` emits base36 names
3. `-0` mode still works under base36

This may require either:

1. loading a config file in the test, or
2. adding a config override path to the CLI invocation pattern already used in tests

#### B2. `utils rpath2apath`

Add cases that verify:

1. base36 regular object parses to expected JSON
2. base36 `D` and `R` parse correctly
3. lowercase `d` and `r` parse and output uppercase flags
4. multiple input lines with mixed formats both work

### C. Mount-facing tests

Primary files:

- `tests/test_mount_view.py`
- `tests/test_mount_cli.py`

Add or extend tests so mixed histories and base36-only histories are visible in the mount view.

Recommended cases:

1. latest-version selection works when only base36 objects exist
2. mixed datetag/base36 directory listing still returns the latest by timestamp
3. delete markers with base36 are respected
4. undated-file tagging behavior remains unchanged

### D. Higher-level backup/refresh/import tests

Primary file candidates:

- `tests/test_backup_restore.py`
- possibly import/refresh tests if they already assert exact `rpath` values

Add at least one production-style test that runs a real backup flow with the config set to `base36-epoch`.

Recommended assertions:

1. created `rpath`s use base36 tokens
2. DB refresh/import can read those paths back into correct `apath`, `timestamp`, and `flag`
3. delete and reference objects also use the new format
4. mixed old/new histories are readable after a config change

This is the most important end-to-end confidence test.

### E. Listing and timestamp aggregation tests

Primary file:

- `tests/test_listing.py`

These may not need many new tests because listings mostly work from integer timestamps in the DB, not from raw filename tags after ingestion.

Still add at least one test proving:

1. base36-named objects refresh/import into correct integer timestamps
2. listing/timestamp output remains human-readable and unchanged

### F. Negative and rejection tests

Add focused failures for malformed base36 tags:

1. 7-character token in timestamp slot
2. 9-character token in timestamp slot
3. invalid chars
4. valid 8-char token plus invalid extra suffix
5. unsupported config value rejected by config validation

## Suggested Concrete Test Cases

Use these exact values to reduce ambiguity:

1. epoch `0` -> `00000000`
2. epoch `1` -> `00000001`
3. epoch `10` -> `0000000a`
4. epoch `1706262301` -> `00s7v4f1`
5. datetag equivalent for `1706262301` -> `20240126094501`

Representative filename cases:

1. `file.txt`
2. `archive.tar.gz`
3. `.dfbempty`
4. `file.19700101000001`
5. `file.jpg.19700101000001`
6. `03/gallery4.png@resize=376,812&ssl=1`

Representative `rpath` cases:

1. `file.00000000.txt`
2. `file.00000001.txt`
3. `file.00s7v4f1.txt`
4. `file.00s7v4f1D.txt`
5. `file.00s7v4f1R.txt`
6. `archive.00s7v4f1.tar.gz`
7. `.dfbempty.00000001`

## Likely Implementation Pitfalls

1. Forgetting to update all write-side `apath2rpath()` callers, leaving mixed output inside a single backup run.
2. Accidentally changing `time2all()` semantics when adding base36 support.
3. Letting lowercase flags leak upward instead of canonicalizing to uppercase.
4. Breaking the current MIME-aware versus fallback split precedence.
5. Making the parser too greedy by scanning arbitrary dot-separated tokens.
6. Failing to keep `rpath2apath()` backward-compatible for existing datetag-only names.
7. Forgetting CLI utility coverage.
8. Forgetting mount behavior, which catches parse regressions in a different way from DB-backed paths.
9. Over-coupling parser behavior to platform datetime conversion limits.
10. Writing docs/spec examples that disagree with implementation test vectors.

## Recommended Execution Checklist

### 1. Add format primitives

- [ ] Add `encode_base36_epoch()`
- [ ] Add `decode_base36_epoch()`
- [ ] Add generic timestamp-tag parser
- [ ] Add tests for these helpers first

### 2. Add config support

- [ ] Add default config setting to `TEMPLATE` in `dfb/configuration.py`
- [ ] Add validation in `_validate()`
- [ ] Add config comments explaining the two values and default

### 3. Update write path

- [ ] Update `apath2rpath()` to accept `fmt=...`
- [ ] Update sanity-check round trip to use the dual-format reader
- [ ] Update all write-side call sites
- [ ] Add unit tests for emitted shape

### 4. Update read path

- [ ] Update `rpath2apath()` to accept `preferred=...`
- [ ] Preserve current split order
- [ ] Add dual-format parse behavior
- [ ] Add lowercase `d`/`r` acceptance with uppercase normalization
- [ ] Add unit regression tests

### 5. Update CLI and consumers

- [ ] Update `cli_apath2rpath()`
- [ ] Update `cli_rpath2apath()`
- [ ] Update mount reader if needed
- [ ] Verify `dstdb.py` and import/refresh readers still behave correctly

### 6. Update docs

- [ ] Update any config template commentary
- [ ] Update end-user docs that mention datetag-only object naming
- [ ] Update CLI help snapshot if necessary
- [ ] Add changelog note if the project wants this recorded there before implementation lands

### 7. Verify

- [ ] Run focused unit tests
- [ ] Run CLI utility tests
- [ ] Run mount tests
- [ ] Run at least one production-style backup/refresh/restore test path
- [ ] Re-read docs examples against actual helper output

## Suggested Test Commands

Exact commands can be adjusted, but these are the likely minimum useful set:

```bash
PYTHONPATH=/home/jwinokur/git_home/dfb pytest tests/test_units.py -k "rpath2apath or apath2rpath or time2all"
PYTHONPATH=/home/jwinokur/git_home/dfb pytest tests/test_cli_utils.py
PYTHONPATH=/home/jwinokur/git_home/dfb pytest tests/test_mount_view.py tests/test_mount_cli.py
PYTHONPATH=/home/jwinokur/git_home/dfb pytest tests/test_listing.py tests/test_backup_restore.py
```

If time is tight, prioritize:

1. `tests/test_units.py`
2. `tests/test_cli_utils.py`
3. one end-to-end backup/refresh/import or backup/restore path

## Done Criteria

This feature is done when all of the following are true:

1. New backups can be configured to write `base36-epoch` object names.
2. Existing datetag backups still behave unchanged by default.
3. Readers accept both formats everywhere that matters.
4. Mixed histories are readable.
5. CLI utility conversions support the new format.
6. Mount view does not regress.
7. Tests cover both focused parsing behavior and real backup flows.
8. Docs and examples match the real helper output.

## Trackable TODO

### Core helpers

- [ ] Add `encode_base36_epoch()` in `dfb/utils.py`
- [ ] Add `decode_base36_epoch()` in `dfb/utils.py`
- [ ] Add generic dual-format timestamp-tag parser helper
- [ ] Keep or rename the old datetag-only helper explicitly
- [ ] Add lowercase-flag read support with uppercase normalization

### Config

- [ ] Choose and add the config key name in `dfb/configuration.py`
- [ ] Add the default value to the config template
- [ ] Add enum validation for `datetag` and `base36-epoch`
- [ ] Add config comments explaining semantics and default

### Write path

- [ ] Add `fmt=` support to `apath2rpath()`
- [ ] Keep default write behavior as `datetag`
- [ ] Update `apath2rpath()` round-trip verification for dual-format parsing
- [ ] Update all backup write call sites to pass the configured format
- [ ] Update any reference-marker write call sites
- [ ] Update any delete-marker write call sites

### Read path

- [ ] Add `preferred=` support to `rpath2apath()`
- [ ] Preserve MIME-aware split as the first parse strategy
- [ ] Preserve plain `os.path.splitext()` fallback as the second parse strategy
- [ ] Try preferred format first within each split strategy
- [ ] Try alternate format second within each split strategy
- [ ] Keep the existing `(apath, ts, flag)` return shape

### CLI utilities

- [ ] Extend `dfb utils apath2rpath` so `--date` accepts datetag input
- [ ] Extend `dfb utils apath2rpath` so `--date` accepts base36-epoch input
- [ ] Add `--format {datetag,base36-epoch}` to `dfb utils apath2rpath`
- [ ] Make `--format` override config for that invocation
- [ ] Keep config as the default output-format choice when `--format` is omitted
- [ ] Extend `dfb utils rpath2apath` to parse both formats
- [ ] Add `dfb utils tsconv`
- [ ] Add `--to {datetag,base36-epoch}` to `dfb utils tsconv`
- [ ] Decide whether `tsconv` should output JSON by default, raw mode optionally, or vice-versa
- [ ] Implement `tsconv` using the shared encode/decode helpers

### Other consumers

- [ ] Update mount view parsing to remain dual-format tolerant
- [ ] Verify `dfb/dstdb.py` refresh/import paths still parse correctly
- [ ] Verify any `__init__` exports remain coherent

### Unit tests

- [ ] Add direct tests for `encode_base36_epoch()`
- [ ] Add direct tests for `decode_base36_epoch()`
- [ ] Add malformed-input tests for base36 decode
- [ ] Add datetag regression tests
- [ ] Add lowercase `d`/`r` parsing tests
- [ ] Add `rpath2apath()` base36 cases
- [ ] Add `apath2rpath(fmt=...)` emission tests
- [ ] Add preferred-first parsing tests
- [ ] Add mixed-format round-trip tests
- [ ] Preserve and extend current MIME-aware edge-case tests

### CLI tests

- [ ] Add `tests/test_cli_utils.py` coverage for `apath2rpath --format datetag`
- [ ] Add `tests/test_cli_utils.py` coverage for `apath2rpath --format base36-epoch`
- [ ] Add `tests/test_cli_utils.py` coverage for `apath2rpath --date` using datetag input
- [ ] Add `tests/test_cli_utils.py` coverage for `apath2rpath --date` using base36 input
- [ ] Add `tests/test_cli_utils.py` coverage for `rpath2apath` on base36 paths
- [ ] Add `tests/test_cli_utils.py` coverage for lowercase flags
- [ ] Add `tests/test_cli_utils.py` coverage for `tsconv`

### Production-style tests

- [ ] Add at least one backup flow test with config set to `base36-epoch`
- [ ] Assert written `rpath`s use base36 tokens
- [ ] Assert delete markers use base36 tokens
- [ ] Assert reference markers use base36 tokens
- [ ] Add at least one mixed-history read test after a format change
- [ ] Add at least one refresh/import-style parse test for base36-named objects
- [ ] Add at least one listing/timestamp regression test proving display output remains human-readable
- [ ] Add mount-facing regression coverage if current tests do not already catch it

### Docs

- [ ] Update config template comments in `dfb/configuration.py`
- [ ] Update `docs/adv_backup_dump_format.md`
- [ ] Update `docs/CLI_help.md` if help output changes
- [ ] Update any user-facing docs that still imply datetag-only object names
- [ ] Re-check `docs/format_spec.md` examples against real helper output after implementation
- [ ] Re-check `docs/base36_epoch_design.md` wording against final implementation choices

### Verification

- [ ] Run focused unit tests
- [ ] Run CLI utility tests
- [ ] Run mount tests
- [ ] Run at least one production-style backup/refresh/restore test path
- [ ] Review final diff for accidental unrelated changes
- [ ] Update `codex/TRANSCRIPT.md` when implementation lands
