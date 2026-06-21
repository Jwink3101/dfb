# DFB Auxiliary Artifact Specification (Draft)

## Status

Draft for review. Defines optional, non-authoritative artifact formats used to enrich or accelerate operations.

## Scope

This specification covers auxiliary JSONL artifacts (logs/snapshots/exports) used by `dfb` tooling.

It does not redefine core backup truth. Core truth remains destination backup objects and reference-file payloads as defined in `docs/format_spec.md`.

## Role and Authority

1. Auxiliary artifacts are optional.
2. Auxiliary artifacts are non-authoritative for core history semantics.
3. Auxiliary artifacts may enrich restoration fidelity (for example mtime/checksum/metadata hints) when backend capabilities are limited.
4. If auxiliary data conflicts with object-derived interpretation, object-derived interpretation wins.
5. Absence or corruption of auxiliary artifacts must not block core content recovery.

## Container Format

1. Line-delimited JSON (JSONL), UTF-8.
2. One JSON object per line.
3. Supported file suffixes:
   1. `.jsonl`
   2. `.jsonl.gz`
   3. `.jsonl.xz`

Readers should treat compression as transport/container only; per-line JSON semantics are unchanged.

## Record Types

Each line MUST be one of:

1. Data record (file/version-like row)
2. Prune action record
3. Comment record

Unknown record shapes should be ignored with warning unless strict mode is requested.

## Data Record

### Required fields

1. `rpath` (string)
2. `apath` (string)
3. `timestamp` (integer epoch seconds)
4. `size` (integer)

### Common optional fields

1. `mtime` (number)
2. `checksum` (object/map)
3. `isref` (boolean or int-compatible value)
4. `ref_rpath` (string)
5. `dstinfo` (boolean or int-compatible value)

### Extension rule

1. Additional unknown keys are allowed.
2. Readers MUST ignore unknown keys they do not understand.

## Prune Action Record

### Required fields

1. `_V` (integer, currently `1`)
2. `_action` with value `"prune"`
3. `rpath` (string)

### Optional fields

1. `size` (integer or null)

### Semantics

Represents deletion intent for `rpath` in import/sync workflows.

## Comment Record

### Required fields

1. `_action` with value `"comment"`

### Optional fields

Any additional fields.

### Semantics

No operational effect. Intended for annotations and ignored by apply/import logic.

## Reconciliation Rules

1. Core object interpretation is authoritative for existence/timeline semantics.
2. Auxiliary data may augment metadata and accelerate resolution.
3. Auxiliary data must not be required to determine whether a version/delete/reference object exists.
4. In conflicts, core object interpretation takes precedence.

## Compatibility and Evolution

1. Auxiliary schema evolution must be additive where possible.
2. Unknown fields must be ignored by default.
3. Unknown action versions (`_V`) should be ignored conservatively with warning unless explicitly supported.
4. New record types should be introduced without breaking readers for existing types.

## Practical Guarantees

1. A user can recover core file contents and logical history without auxiliary artifacts.
2. Auxiliary artifacts can improve restoration fidelity when backend/source attributes are missing or degraded.
3. Tooling may choose not to replay all auxiliary attributes even when present; this does not invalidate core recovery guarantees.

