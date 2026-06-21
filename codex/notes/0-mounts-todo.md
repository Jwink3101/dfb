# Mount Reimplementation TODO

- Review existing `dfbmount/mount.py` behavior and failure points with FUSE/FUSE-T.
- Define clean path mapping: rclone mount path -> apath grouping, time filtering, and reference resolution.
- Rework directory listing to avoid deep recursion and improve empty-dir handling.
- Build a mount module that does not embed FUSE code directly (use external dependency if needed).
- Ensure compatibility with libfuse2 on Linux and FUSE-T on macOS.
- Update CLI entry point and docs for new mount behavior.
- Add tests or manual validation notes for mount edge cases.
