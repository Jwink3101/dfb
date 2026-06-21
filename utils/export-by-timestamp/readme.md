# Export by Timestamps

A small utility script to export a database by the timestamps themselves. It incrementally exports the dfb database such that it can be regenerated.

See the `--help` docs for details on how it handles existing exports.

**Note**: These files will be similar to `.dfb/snapshots` files but **not the same**. Those files a based on when it was run and include prune actions. They do not change and instead, when combined, represent the snapshot. These files will be affected by prune and represent the current state.

Must have `dfb` installed or in the path. Must have JSONLiteDB. 

    $ pip install jsonlitedb

## STATUS: Alpha and rough

This script is "alpha" at best and very, very rough. Use with caution