# Mount (EXPERIMENTAL)

In its own package is `dfb-mount` installed along with `dfb`. This is **EXPERIMENTAL** at best. It overlays *an rclone mount* and presents the latest (or set) version of the files.

Some notes:

- It does not serve files directly. It overlays an rclone mount. Use `rclone mount --vfs-cache-mode full` for best effect (especially since it will read the remote a lot). Rclone is way better suited for serving the files
- It is **stateless** (except for an optional cache). It doesn't use the remote database and need-not point to a single backup. You can mount many backups (or the top level of one with many backup destinations) and it'll work just fine. If it can't parse a date, it just provides the file and doesn't do any grouping.
- Unless using the `--remove-empty-dirs`, empty directories, such as from deleted files, will be shown. Determining if a directory is empty requires walking until it either finds a file or all the way until it doesn't. It is suggested to use the cache with this. You can always use a short cache duration.
- Logging is incomplete. I still need to fix this
- Does not resolve symlink files.
- Just to repeat, **THIS IS EXPERIMENTAL**. 
- Does **not work with libfuse3**. Use libfuse2:
    ```
    $ sudo apt-get update -y
    $ sudo apt-get install -y libfuse2
    ```