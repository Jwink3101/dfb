# Remove `--shell-script` TODO

## Purpose

This file captures the agreed plan to remove `--shell-script` completely from dfb so the work can continue even if the conversation history is lost.

This is a hard removal, not a deprecation.

## Agreed Decisions

These points were explicitly agreed with the user:

- `--shell-script` should be hard-removed with no compatibility layer and no deprecation period.
- The removal is complete: CLI flag, restore code path, helper APIs, tests, docs, and internal notes.
- There is no replacement feature in this change.
- Do not add a test that specifically checks rejection of `--shell-script`.
- Delete duplicate restore shell-script coverage instead of rewriting it as normal restore coverage.
- Remove internal notes that mention the mode; the only intentional remaining mention should be a historical note in the changelog.
- Add a changelog note calling out the breaking CLI removal.
- Remove the low-level `RcloneCLI.capture().shell_script()` helper and any now-dead stdin-capture support tied only to that helper.

## Scope Summary

Expected files or areas to touch:

- `dfb/cli.py`
- `dfb/restore.py`
- `dfb/rclonecli.py`
- `tests/test_backup_restore.py`
- `tests/test_rclonecli.py`
- `docs/CLI_help.md`
- `docs/symlink_update.md`
- `docs/metadata_restore.md`
- `docs/changelog.md`
- `codex/notes/metadata/design.md`
- `codex/notes/metadata/todo.md`
- `codex/notes/metadata/implementation_notes.md`
- `codex/TRANSCRIPT.md`

Search again before finalizing in case additional references appeared.

## Detailed TODO

### 1. Reconfirm reference set before editing

- [x] Run a repository search for `--shell-script`, `shell_script`, and `shell-script`.
- [x] Separate real source/doc references from generated artifacts like `tests/htmlcov`.
- [x] Confirm there are no remaining production callers of `RcloneCLI.capture().shell_script()`.
- [x] Confirm whether `save_stdin` and `command_stdin` have any purpose after helper removal.

### 2. Remove the restore CLI surface

- [x] Remove the shared `exe_shell_parent` parser or reduce parser construction so restore commands no longer receive `--shell-script`.
- [x] Update `restore-dir` parser parents in `dfb/cli.py` to stop inheriting shell-script arguments.
- [x] Update `restore-file` parser parents in `dfb/cli.py` to stop inheriting shell-script arguments.
- [x] Keep other execution controls such as `--dry-run` and `--interactive` intact.
- [x] Re-read surrounding parser code after edits to ensure no orphaned parser-group setup remains.

### 3. Remove restore implementation branches

- [x] Delete the `self.args.shell_script` branch in `Restore.__init__` flow so restore always uses the real transfer path.
- [x] Remove `Restore.transfer_shell()` entirely from `dfb/restore.py`.
- [x] Remove imports used only by shell-script generation from `dfb/restore.py`.
- [x] Remove logging that special-cases restore metadata behavior for shell-script mode.
- [x] Re-check restore flow to ensure normal restore behavior, dry-run behavior, interactive behavior, and stdout restore behavior remain unchanged.

### 4. Remove low-level shell-script helper code

- [x] Delete `capture.shell_script()` from `dfb/rclonecli.py`.
- [x] Remove `save_stdin` support from `RcloneCLI.capture(...)` if it is dead after helper removal.
- [x] Remove `command_stdin` state and related bookkeeping if it is no longer used.
- [x] Update capture class docstrings/comments to match the reduced API.
- [x] Re-read capture call sites to ensure `save_results` behavior still works and no signatures are left misleading.

### 5. Rewrite tests by deletion, not replacement

- [x] Delete the restore shell-script loop from `tests/test_backup_restore.py`.
- [x] Delete the restore-file stdout shell-script invocation from `tests/test_backup_restore.py`.
- [x] Keep the ordinary restore assertions that already cover restored end-state correctness.
- [x] Remove the `cap.shell_script()` assertions from `tests/test_rclonecli.py`.
- [x] Remove any test setup that existed only to support shell-script generation, including stdin-capture expectations if no longer needed.
- [x] Do not add a new test asserting that `--shell-script` is rejected.
- [x] Re-scan tests for commented or disabled shell-script references and remove stale ones if they are no longer useful.

### 6. Clean user-facing docs

- [x] Remove restore `--shell-script` references from `docs/CLI_help.md`.
- [x] Update any restore help text snapshots so usage blocks and option descriptions no longer mention the flag.
- [x] Remove the shell-script limitation bullet from `docs/metadata_restore.md`.
- [x] Update `docs/symlink_update.md` so it no longer claims special behavior for regular versus shell-script restore modes.
- [x] Search the rest of `docs/` for any remaining mention and remove it unless it is the changelog entry.

### 7. Clean internal notes

- [x] Remove stale shell-script limitation/design references from `codex/notes/metadata/design.md`.
- [x] Remove stale shell-script limitation/design references from `codex/notes/metadata/todo.md`.
- [x] Remove stale shell-script limitation/design references from `codex/notes/metadata/implementation_notes.md`.
- [x] Confirm no other `codex/notes/` files mention the removed mode.

### 8. Add historical changelog note

- [x] Add a short entry to `docs/changelog.md` explicitly stating that restore-side `--shell-script` support was removed.
- [x] Make the note clearly breaking in tone so existing automation users understand the CLI changed.
- [x] Ensure this is the only intentional remaining mention of `--shell-script` in docs/internal notes.
  Remaining non-generated mentions are limited to `docs/changelog.md` plus this removal handoff note and its transcript entry.

### 9. Verify behavior

- [x] Run focused searches again to confirm production/docs/tests no longer mention `--shell-script` or `shell_script`, except the changelog entry and any unavoidable generated artifacts.
- [x] Run the most relevant tests for restore and rclone helper behavior.
- [x] If full coverage is too expensive, run the smallest subset that covers edited behavior and record what was run.
  Ran `PYTHONPATH=/home/jwinokur/git_home/dfb pytest tests/test_rclonecli.py -k test_main` and `PYTHONPATH=/home/jwinokur/git_home/dfb pytest tests/test_backup_restore.py -k test_main`.
- [x] Check that CLI help generation or snapshots remain consistent after parser changes.

### 10. Final cleanup

- [ ] Update `codex/TRANSCRIPT.md` with a one-line changelog entry including date and time.
- [x] Review `git diff` for accidental unrelated edits.
- [x] Make sure no dead imports, dead comments, or misleading docstrings remain.

## Risks To Watch

- Removing parser wiring may leave unused parser objects or argument-group setup behind in `dfb/cli.py`.
- Restore metadata work recently added shell-script-specific wording; that text must be removed cleanly without changing metadata behavior for normal restores.
- `RcloneCLI.capture(...)` may have dead parameters after helper removal; leaving them behind would preserve misleading internal API surface.
- `docs/CLI_help.md` appears to be a generated or maintained snapshot; if it is regenerated, the generating workflow may also need to be rerun.
- The repo contains coverage HTML with historical strings; decide whether to ignore generated artifacts or refresh/remove them based on normal repo practice.

## Suggested Execution Order

1. Remove CLI/parser wiring.
2. Remove restore implementation branch.
3. Remove low-level helper API.
4. Delete dependent tests.
5. Clean docs and internal notes.
6. Add changelog note.
7. Run focused verification.
8. Update transcript.
