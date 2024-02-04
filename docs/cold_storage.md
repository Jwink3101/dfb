# Using dfb with cold storage

***

## WARNING -- Advanced Usage

This is a guide on how you can use dfb with cold storage but it assumes some more advanced knowledge may be needed, especially around Python, than regular usage.

***

Because dfb is append only, it is possible to make it work with cold storage. It is far from perfect and, as noted in the "Warning" admonition above, it assumes some advanced knowledge

The basic premise is that you upload to "online" storage, do an export (either of the newest or everything), then move that to "offline" or "cold" storage. If you ever need to refresh, you do a `dbimport` after. If you need to restore, you do it manually based on snapshots.

## (Optional) Disable Refresh

A regular refresh will break this workflow since the online storage won't have everything. So we use the [advanced setting](adv_settings.md):
```python
disable_refresh = True
```
in your config.

When you ever need to refresh or import, you will set `--override 'disable_refresh = False'` in the CLI

## Backup

I will assume you have your hot storage configured in your backup. Do normal backups until it is time to archive.

Let's say you are archiving for the first time on "2023-01-20T16:37:53-07:00"

Move your hot storage to backup then run an export 
```
dfb snapshot \
    --config config.py \
    --export \ 
    --before "2023-01-20T16:37:53-07:00" \
    --output "0000-00-00.2024-01-20-cold0.jsonl"
```

Now, lets say some time later, you wish to move files again. You again move to a new cold storage location and do an export. Note that there is no *need* to do an `--after` but it (a) reduces the file size and (b) can be used more easily later to identify *where* files are in cold storage. This is up to you. Note that the `--after` is incremented by a second. Say it is currently "2023-09-04T10:21:32-06:00"

```
dfb snapshot \
    --config config.py \
    --export \
    --after "2023-01-20T16:37:54-07:00" \
    --before "2023-09-04T10:21:32-06:00" \
    --output "2024-01-20.2024-09-04.cold1.jsonl"
```

And this can continue

## Refreshing

To refresh, you would do:

    dfb refresh --config config.py --override "disable_refresh = False"
    dfb advanced dbimport \
        --config config.py \
        --override "disable_refresh = False" \
        "0000-00-00.2024-01-20-cold0.jsonl" \
        "2024-01-20.2024-09-04.cold1.jsonl"

Note that order *does matter* if you have any prune actions but otherwise doesn't. This is because the prune will delete the entry from the database which it can't do it added in the wrong order.

## Prune

Pruning follows a similar path to backup. First run prune with `--dump`. This will return line-delimited json like

```json
{"_V":1,"_action":"prune","rpath":"<file>"}
```

After deleteding `<file>` from cold (or active) storage, it can be applied with `advanced dbimport` and/or the file can be removed from the `jsonl` files and the entire database can be refreshed.

**Note**: It is possible that the `rpath` is actually the path to a reference file. The entry *makes no distinction*. If applied to the reference file, nothing will happen in the DB but it symbolizes that the reference should be removed (and presumably, so as not to break anything, the referent is also removed). 

## Restore

Restore should just be done with `snapshot` and then those files are manually downloaded

## Resouces

See [documentation on the JSONL formats used](adv_backup_dump_format.md) for more.
