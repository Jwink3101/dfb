# Changelog

## 20230606.0

- Added the option to track moves and use a [server-side] copy instead of a reference. This only makes sense when the remote support server-side copy (not verified by dfb). It enables a cleaner representation without using bandwidth
- Tests for new capability

## 20230508.0

- Fixed removed feature.

## 20230507.0

- Adds the `disable_prune` (default to False) configuration option so that it is harder to accidentally prune. Makes it effectively append-only unless very, very explicit.
- Minor documentation updates

## 20230502.0

- Can specify `--deleted` (or `--del`) twice for `ls` and `snaphot` to *only* include deleted items

## 20230501.0

- Adds version count to `ls -ll`

## 20230429.0

- Adds the option to upload snapshot files with each run. These are functionally the same as calling `snapshot --only <timestamp> --deleted`
- Added `--deleted` to snapshot
- Bug Fixes
    - source and dest labels mixed up in restore logs

## 20230411.0 (BETA)

- Bug Fix

## 20230407.0 (BETA)

- If any part fails, it'll keep going until deletes and references happen
- Adds pruning only a subdir. Still have to compute all prunes to ensure there aren't references but will then filter them.

## 20230402.0 (BETA)

- Adds support for symlinks on a local source. It is not super robust but it works for most cases. Note that dfb does not restore the symlinks as symlinks. It restores them as `.rclonelink` files which can be easily turned into symlinks later. See an example in the readme

## 20230331.0 (BETA)

- **EXPERIMENTAL** FUSE mount that overlays an rclone mount (does not serve the remote directly)
- Cleanup, documentation, etc
- Fixed version numbering to validate PEP440 and other build errors.
    - This is still beta, but just isn't in the date-based version number

## 20230330.0.BETA

- Changed how references are written and saved. The change will not break existing references but older references will not work in the upcoming mounting tools
- Minor other updates

## 20230327.0.BETA

- 7-10x speed up of `ls` command, especially for larger backups.
- Readme clarity

## 20230326.0.BETA

Initial release