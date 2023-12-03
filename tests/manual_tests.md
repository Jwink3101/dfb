# Manual Tests

These are situations that are just too hard to script but need to make sure they work properly.

## Permissions with B2 (and S3)

Run the test suite (or at least the metadata one). I am assuming you have a remote called `b2:` and `b2s3:` with a bucket (I wrote `<bucket>`)

Copy it

    $ rsync -a testdirs/metadata/src metadata/
    $ cd metadata
    
Create a config: `configb2.py`

```python
src = "src"
dst = "b2:jgwrclonetest/dstb2"
rclone_env = {
    "RCLONE_CACHE_DIR": "cache"
}
_uuid = "metab2"
metadata = True
```

Run it

    $ dfb backup --config configb2.py
    $ dfb restore --config configb2.py restoreb2

Create a config: `configs3.py`

```python
src = "src"
dst = "b2s3:jgwrclonetest/dsts3"
rclone_env = {
    "RCLONE_CACHE_DIR": "cache"
}
_uuid = "metas3"
metadata = True
```

Run it

    $ dfb backup --config configs3.py
    $ dfb restore --config configs3.py restores3
    
Then verify permissions!

**NOTE**: As of testing (2023-12-03), B2 does not seem to work but it does via S3...