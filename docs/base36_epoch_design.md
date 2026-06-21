# Base36 Epoch Filename Timestamp Design

## Status

Proposed design for a backward-compatible optional filename timestamp encoding in backup object names.

This document records the design intent, rationale, tradeoffs, and compatibility expectations for `base36-epoch`. The normative object-format rules belong in [docs/format_spec.md](./format_spec.md).

## Summary

`dfb` currently writes object names using a UTC `datetag` in `YYYYMMDDhhmmss` form.

This design adds an optional alternate encoding, `base36-epoch`, for the same timestamp semantics:

- `datetag`: fixed-width 14-character UTC timestamp token
- `base36-epoch`: fixed-width 8-character lowercase base36 encoding of unsigned UTC epoch seconds

The goal is not to change timestamp meaning. The goal is to provide a shorter object-name encoding for path-length-sensitive backups while preserving deterministic parsing and within-format lexical chronological ordering.

## Goals

1. Preserve backward readability for existing backups.
2. Keep the new format optional and explicit.
3. Shorten the filename timestamp token from 14 characters to 8 characters.
4. Remain filename-safe and case-insensitive on read.
5. Preserve natural lexicographic ordering within the new format.
6. Avoid introducing new timestamp semantics beyond UTC epoch seconds.

## Non-Goals

1. Improving human readability over `datetag`.
2. Preserving lexicographic chronological ordering across mixed encodings.
3. Providing forward compatibility with older `dfb` versions.
4. Changing auxiliary-log formats, database layout, or CLI timestamp input syntax.
5. Adding sub-second precision to object names.

## Chosen Design

### Encoding

`base36-epoch` is defined as:

- unsigned UTC epoch seconds since `1970-01-01T00:00:00Z`
- alphabet `0123456789abcdefghijklmnopqrstuvwxyz`
- fixed width 8
- lowercase canonical write form
- big-endian positional encoding
- left zero-padding with `0`

The valid numeric domain is the full inclusive range represented by `[00000000, zzzzzzzz]`.

### Semantics

`datetag` and `base36-epoch` encode the same UTC point-in-time semantics. The choice affects only object-name representation.

### Flags

The existing flags are retained:

- `""`: regular object
- `D`: delete marker
- `R`: reference marker

Canonical writing remains uppercase `D` and `R`. Readers should accept lowercase `d` and `r` as a compatibility expansion.

### Configuration

The preferred filename timestamp format is configured per backup configuration.

Supported values:

- `datetag`
- `base36-epoch`

The exact config variable name is implementation detail and is intentionally not fixed by this design document.

### Writing

Writers emit the configured preferred format for new objects only.

Changing the configured format:

- affects future writes only
- does not rename existing objects
- allows mixed-format histories

### Reading

Readers always support both formats.

Read-time parse order is deterministic:

1. Try the existing MIME-aware split strategy.
2. If that fails, try the existing non-MIME-aware last-dot split strategy.
3. Within each split strategy, try the configured preferred format first.
4. If that fails, try the alternate format.
5. Stop at the first successful interpretation.

If no interpretation succeeds, the object remains non-conforming or undated as in the current model.

When config is absent, unknown, or invalid, `datetag` is treated as the preferred format.

## Rationale

### Why an alternate encoding at all

The main motivation is path-length-sensitive backups. Reducing the timestamp token from 14 characters to 8 characters lowers object-name length without changing object semantics.

### Why keep `datetag` as the default

`datetag` is more human-readable and is already the established format. Keeping it as the default is the least surprising rollout and preserves compatibility with older tooling for users who do not opt in.

### Why base36 instead of a prefixed encoding

No explicit marker was chosen. The grammar already has an established timestamp slot, and deterministic parsing can be preserved by:

- using fixed width
- using the existing split rules
- trying the configured preferred format first
- trying the alternate format second

This accepts a bounded amount of ambiguity similar to the existing date-like token behavior.

### Why exactly 8 characters

Fixed width keeps the format simple and preserves within-format lexicographic chronological ordering. A variable-width encoding would make sorting and parsing less predictable.

### Why lowercase write and case-insensitive read

Lowercase write form keeps object names stable and consistent. Case-insensitive read preserves portability and tolerance for manual edits or case-normalizing environments.

### Why no sub-second precision

The current object-name contract is second-resolution. Keeping `base36-epoch` at epoch-second resolution makes it an alternate wire representation rather than a semantic change.

## Tradeoffs

### Benefits

1. Shorter timestamp token in object names.
2. Deterministic parse rules without adding a prefix.
3. Natural lexical sort within the new encoding.
4. Full backward readability in new tooling.
5. Manual recoverability remains possible because the encoding is fully specified.

### Costs

1. Reduced immediate human readability compared with `datetag`.
2. Mixed-format histories do not preserve lexicographic chronological ordering across the two encodings.
3. Older `dfb` versions may not recognize `base36-epoch` objects.
4. Bare base36 in the timestamp slot can be ambiguous in some manually-created names, just as date-like tokens already can be.

## Compatibility Expectations

### Backward compatibility

Newer `dfb` versions should read both `datetag` and `base36-epoch`.

### Forward compatibility

Older `dfb` versions that predate `base36-epoch` may fail to interpret newer objects correctly. This is expected incompatibility, not a format defect.

### Mixed histories

Mixed histories are valid when users change the configured format on an existing backup. Users accept the operational consequence that backend or browser lexical listing order will no longer match chronological order across the mixed encodings.

## Human Recoverability

`datetag` remains the more eyeball-friendly format and should stay the default for that reason.

`base36-epoch` is still manually recoverable because:

1. the token width is fixed
2. the alphabet is fixed
3. the positional encoding is fixed
4. the parse order is defined
5. the value maps directly to UTC epoch seconds

## Required Spec Content

The normative spec should include:

1. both valid timestamp-tag encodings
2. canonical write forms
3. accepted read forms
4. deterministic parse-order pseudocode
5. base36 encode/decode pseudocode
6. exact examples, including `00000000`, `00000001`, `D`, `R`, and a mixed-ordering note

