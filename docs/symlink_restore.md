# Symlink Restore

For local sources, you can configure how to handle links. Due to an [rclone bug #6855](https://github.com/rclone/rclone/issues/6855), dfb handles symlinks on its own for backups. This works for both the regular and `--shell-script` modes. However, dfb *does not support restore of symlinks*. After restore, there will be `.rclonelink` files that used to be symlinks. These can be manually (or simply Python script) fixed later:

```python
from pathlib import Path

RESTORE_DIR = "."

links = Path(RESTORE_DIR).rglob("*.rclonelink")
for link in links:
    dst = str(link).removesuffix(".rclonelink")
    src = link.read_text()
    Path(dst).symlink_to(src)
    # link.unlink()  # OPTIONAL
```

Note that this will fail if for some reason there are files called `.rclonelink` that aren't intended to be links