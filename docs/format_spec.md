# DFB Backup Object Format Specification (Draft)

## Design Preamble

This format is intentionally designed so backup data can be understood and recovered by humans without requiring `dfb` itself.

The intended properties are:

1. Human-understandable object naming and semantics.
2. Human-auditable history (what existed, changed, moved, or was deleted).
3. Human-recoverable data with generic tools plus documented rules.
4. Tool-optional operation: `dfb` is a convenience layer, not a hard dependency.
5. Manual manipulation is possible, but may create ambiguities or broken references if done without care.

## Status

Draft for review. Intended to define a stable, tool-optional format contract for backups written by `dfb` and interpreted by compatible tools.

Design rationale for the optional `base36-epoch` encoding is documented separately in [docs/base36_epoch_design.md](./base36_epoch_design.md).

## Scope

This specification defines how backup object names and reference-file contents are interpreted.

It does not require any particular command-line interface, database layout, or local cache implementation.

Optional auxiliary artifacts (logs/snapshots/exports) are specified separately in `docs/aux_format_spec.md`.

## Goals

1. Backups are human-interpretable and auditable.
2. Backups are restorable without proprietary indexes.
3. Existing backups remain readable across tool versions.
4. The object set is primary truth; optional logs/snapshots may enrich restoration but are not required for core recovery.

## Core Principles

1. Backup objects on the destination are the authoritative source of file history.
2. Optional logs/snapshots are auxiliary and non-authoritative for core existence/history semantics.
3. If auxiliary data conflicts with object-derived history, object-derived history wins.
4. `apath` reconstruction must remain stable across parser/writer evolution.

## Terminology

1. `apath`: Apparent path (logical/original path).
2. `rpath`: Real path stored on destination.
3. `timestamp tag`: Filename timestamp token stored in the `rpath`.
4. `datetag`: UTC timestamp tag `YYYYMMDDhhmmss`.
5. `base36-epoch tag`: UTC epoch-seconds timestamp tag encoded as fixed-width 8-character base36.
6. `flag`: Optional suffix on a timestamp tag: `""`, `"D"`, `"R"`.
7. `regular object`: Object with empty flag.
8. `delete marker`: Object with `D` flag.
9. `reference marker`: Object with `R` flag whose content points to another object.

## Timestamp Tags and Flags

Valid timestamp tag encodings:

1. `datetag`
2. `base36-epoch tag`

### `datetag`

Valid `datetag` token:

1. Exactly 14 digits.
2. Parsed as `YYYYMMDDhhmmss` in UTC.

### `base36-epoch tag`

Valid `base36-epoch tag` token:

1. Exactly 8 base36 characters.
2. Uses alphabet `0123456789abcdefghijklmnopqrstuvwxyz` for canonical write form.
3. Is interpreted case-insensitively on read.
4. Is parsed as unsigned UTC epoch seconds since `1970-01-01T00:00:00Z`.
5. Has valid numeric range inclusive of `[00000000, zzzzzzzz]`.

### Flags

Valid canonical flag tokens:

1. `""`
2. `"D"`
3. `"R"`

Accepted noncanonical read-only flag tokens:

1. `"d"`
2. `"r"`

Note:

1. Current code behavior recognizes uppercase `D` and `R` only.
2. Lowercase `d` and `r` are specified here as a compatibility expansion for readers.

Valid full tag grammar:

1. `<tag> := <timestamp-tag><flag?>`
2. `<timestamp-tag> := <datetag> | <base36-epoch-tag>`
3. `<flag?> := "" | "D" | "R"`

Any token not matching this shape is not a valid `dfb` tag.

## Canonical Write Form

Writers should emit:

1. `rpath = <base> + "." + <timestamp-tag><flag> + <ext>`

where:

1. `<base>, <ext>` come from canonical split rules below.
2. `<timestamp-tag>` is chosen from the configured preferred format.

Supported preferred formats:

1. `datetag`
2. `base36-epoch`

If preference is absent, unknown, or invalid, `datetag` is the default preferred format.

Examples:

1. `file.txt -> file.20260101010203.txt`
2. `archive.tar.gz -> archive.20260101010203.tar.gz`
3. `file.txt` delete marker -> `file.20260101010203D.txt`
4. `file.txt` reference marker -> `file.20260101010203R.txt`
5. `empty/.dfbempty -> empty/.dfbempty.20260101010203`
6. `file.txt -> file.00000000.txt`
7. `file.txt -> file.00000001.txt`
8. `file.txt` delete marker -> `file.0000000aD.txt`
9. `file.txt` reference marker -> `file.0000000aR.txt`
10. `file.txt -> file.20240126094501.txt`
11. `file.txt -> file.00s7v4f1.txt`

## Canonical Split Rules (`apath` -> `base/ext`)

Canonical split is MIME-aware, matching `smart_splitext` behavior:

1. Split filename by dots.
2. Treat rightmost token sequence as extension chain while moving left through known MIME extensions.
3. Preserve case of original name.
4. Support dotfiles and no-extension names.

Note:

1. MIME knowledge may evolve.
2. This may change future emitted `rpath` shape for some edge names.
3. This must not break reader compatibility or historical `apath` interpretation.

## Reader Compatibility Rules (`rpath` -> `apath, timestamp, flag`)

Reader must accept both modern and legacy forms.

Reader must accept both timestamp tag encodings regardless of configured preferred format.

Parsing order:

1. Attempt MIME-aware split parse first.
2. If that parse fails, attempt fallback last-dot parse.
3. Within each split attempt, try the configured preferred timestamp format first.
4. If that parse fails, try the alternate timestamp format.
5. If both split strategies fail, the object is non-dated/non-conforming.

This tolerance is required for backward compatibility and for manually-created edge names.

## Parse Pseudocode

```text
parse_rpath(rpath, preferred="datetag"):
    for split_strategy in [mime_aware_split, fallback_last_dot_split]:
        base, candidate_tag, ext = split_strategy(rpath)
        if split failed:
            continue

        for fmt in [preferred, alternate(preferred)]:
            parsed = parse_tag(candidate_tag, fmt)
            if parsed succeeded:
                timestamp, flag = parsed
                apath = base + ext
                return (apath, timestamp, flag)

    fail as non-conforming or undated
```

```text
parse_tag(candidate_tag, "datetag"):
    if candidate_tag matches exactly 14 digits plus optional trailing D/R/d/r:
        parse first 14 digits as UTC YYYYMMDDhhmmss
        parse optional trailing flag
        return (epoch_seconds, canonicalized_flag)
    else:
        fail
```

```text
parse_tag(candidate_tag, "base36-epoch"):
    split candidate_tag into:
        timestamp_token = first 8 characters
        optional_flag = remaining characters

    if timestamp_token is not exactly 8 base36 characters:
        fail

    if optional_flag not in ["", "D", "R", "d", "r"]:
        fail

    timestamp = decode_base36_epoch(timestamp_token)
    return (timestamp, canonicalized_flag)
```

## `base36-epoch` Encode/Decode

Canonical encoding uses ordered lowercase alphabet:

1. `0123456789abcdefghijklmnopqrstuvwxyz`

Encoding pseudocode:

```text
encode_base36_epoch(epoch_seconds):
    require 0 <= epoch_seconds <= value("zzzzzzzz")
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
    if epoch_seconds == 0:
        return "00000000"

    digits = []
    n = epoch_seconds
    while n > 0:
        n, remainder = divmod(n, 36)
        digits.append(alphabet[remainder])

    token = join(reverse(digits))
    return left_pad_with_zero(token, width=8)
```

Decoding pseudocode:

```text
decode_base36_epoch(token):
    require len(token) == 8
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
    token = lowercase(token)

    value = 0
    for ch in token:
        digit = index_of(ch in alphabet)
        if digit is invalid:
            fail
        value = value * 36 + digit

    return value
```

## Ordering

1. Within `datetag`, lexicographic order matches chronological order.
2. Within fixed-width `base36-epoch`, lexicographic order matches chronological order.
3. Across mixed encodings, lexicographic order is not defined to match chronological order.

Mixed-ordering note:

1. `file.20240126094501.txt` and `file.00s7v4f1.txt` can represent the same timestamp.
2. Mixed histories remain valid, but plain lexical listing across the two encodings is not a time sort.

## Required Invariants

1. For any writer-produced `rpath`, parser must recover original tuple `(apath, timestamp, flag)`.
2. `apath` stability is mandatory across tooling versions.
3. `rpath` shape is not identity; interpretation result `(apath, timestamp, flag)` is identity.
4. `datetag` and `base36-epoch` encode the same UTC point-in-time semantics.
5. Parser/writer changes must not invalidate old backup readability.

## Object Semantics

### Regular Object (`flag=""`)

Represents file content version at `timestamp`.

### Delete Marker (`flag="D"`)

Represents logical deletion at `timestamp`.

Marker file content is not semantically significant.

### Reference Marker (`flag="R"`)

Object content encodes reference to a referent object.

Reference payload versions:

1. V2 canonical JSON: `{"ver":2,"rel":"<relative-path-to-referent-rpath>"}`
2. V1 legacy plain text path (implicitly version 1)

Reader requirements:

1. MUST parse V2.
2. MUST parse V1 for compatibility.
3. SHOULD report unresolved/missing referents clearly.

Writer recommendations:

1. SHOULD emit V2 only.

### Empty Directory Marker

`.dfbempty` is a normal `apath` name with special restoration meaning by tooling.

Its presence enables preserving empty directories on backends without directory semantics.

## Non-Conforming and Undated Objects

Objects that do not parse into valid `(apath, timestamp, flag)` are outside core format.

Tooling may:

1. Ignore them.
2. Show/tag them as undated.

Such objects must not corrupt dated timeline interpretation.

## Auxiliary Artifacts (Optional Logs/Snapshots)

Auxiliary artifacts are allowed and encouraged for richer restoration.

Examples:

1. Recovering modtimes when backend cannot preserve/return them.
2. Recovering checksums or metadata hints for faster/more-complete restores.

Normative rules:

1. Auxiliary artifacts MUST be optional for core content recovery.
2. Auxiliary artifacts MAY improve restoration fidelity.
3. Core restoration fidelity is bounded by backend capabilities and available data.
4. Tooling may choose not to restore certain attributes even if present.

## Compatibility and Evolution

1. Format evolution must be additive and backward readable.
2. Old objects remain valid without migration.
3. New tooling must preserve historical parser tolerance.
4. New tooling must read both `datetag` and `base36-epoch`.
5. Older tooling may not understand `base36-epoch`; this is expected incompatibility rather than a format defect.
6. Any future reference payload version must remain distinguishable and compatible via explicit `ver`.

## Rationale Summary

1. `datetag` remains the default because it is more human-readable.
2. `base36-epoch` is a formal optional encoding for path-length-sensitive backups.
3. `base36-epoch` is not intended to improve immediate readability.
4. Both formats remain manually recoverable because the filename semantics are fully specified.
5. Mixed-format histories are valid, but they lose cross-format lexicographic chronological ordering.

## Edge Cases Covered by This Contract

1. Multiple date-like or base36-like tokens in a filename.
2. Dotfiles and no-extension names.
3. Multi-extension names (`.tar.gz`, mixed case).
4. Legacy placement of timestamp tag at end.
5. Existing timestamp-like suffix in original filename.
6. Ambiguous MIME/timestamp mixes from manual edits.
7. `.dfbempty` markers.
8. V1 and V2 reference files.
9. Missing/broken references.
10. Mixed histories across `datetag` and `base36-epoch`.

## Out of Scope

1. CLI syntax and flags.
2. Local database schema.
3. Log/snapshot file schema details.
4. Concurrency/operational guidance.
5. Exact implementation config variable name.

## Implementation Mapping (Current Code)

The behavior described here maps to current conversion logic:

1. `dfb/utils.py::apath2rpath`
2. `dfb/utils.py::rpath2apath`
3. `dfb/utils.py::smart_splitext`
4. `dfb/utils.py::parse_dateflag`

Compatibility examples and edge-case expectations are reflected in:

1. `tests/test_units.py` (`test_smart_splitext`, `test_rpath2apath`, `test_apath2rpath`)
2. `docs/adv_backup_dump_format.md` (narrative examples)
