# Symlink Update

As of version `202400428.0`, a semi-breaking change was introduced to how symlinks are handled. Most notably, **dfb now matches rclone's behavior** for how it handles symlinks. That is, if the destination is local, it will make the link. Otherwise, will make the `<name>.rclonelink` file. Previous versions of dfb *always* wrote a `<name>.rclonelink` file. Furthermore, setting `links` in the config is deprecated. Instead, the flag should be set in `rclone_flags`

| Previous Link Setting | New `rclone_flags` item | Behavior                                                              |
|-----------------------|--------------------------|-----------------------------------------------------------------------|
| `links = 'link'`      | `'--links'`              | Make symlink if `dst` is local otherwise make `<name>.rclonelink` file |
| `links = 'copy'`      | `'--copy-links'`         | Copy the referent                                                     |
| `links = 'skip'`      | `'--skip-links'`         | Skip all together                                                     |


Because prior versions always wrote `<name>.rclonelink` files even if the destination was local, this is a breaking change.

## Update the repository

The follow Python snippet will update the destination appropriately. 

```python
DEST = "<SPECIFY DEST>"

from pathlib import Path

files = Path(DEST).rglob('*.rclonelink')

for file in files:
    ref = file.read_text()
    new = file.parent / file.name.removesuffix('.rclonelink')
    new.symlink_to(ref)
    file.unlink() # Optional
```

## Why make this change

There are a few reasons. The biggest is [Issue #1](https://github.com/Jwink3101/dfb/issues/1) where dfb couldn't read the symlinks in wrapper remotes and was then compounded by rclone's behavior when you try to write `.rclonelink` files from the `rc` interface on local.

Also, the resulting behavior now **matches rclone's**. And the [limiting rclone bug #6855](https://github.com/rclone/rclone/issues/6855) has long been fixed.

---

---

DEPRECATED Below

---

---

## Symlink Restore on prior versions

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