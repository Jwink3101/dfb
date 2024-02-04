# Advanced (hidden) settings

There are some settings not in the CLI or config file template that can be set in your config (and/or done with `--override`).

- `dbcache_dir = None`: Where to store the file database. Default is `<rclone cache dir>/DFB/<_uuid from config>.db`
- `disable_refresh = False`: Do not allow refresh