#!/usr/bin/env python
import argparse
import logging as log
import subprocess
from pathlib import Path

# from dfb.utils import smart_open # imported if count or count-cache
# from jsonlitedb import JSONLiteDB # imported if count-cache

__version__ = "20251108.0"

## CLI
parser = argparse.ArgumentParser(description="Export database by timestamps")

parser.add_argument("config", metavar="dfb-config", help="Path to the dfb config file")
parser.add_argument(
    "dest", metavar="export-dir", type=Path, help="Path to export directory"
)

parser.add_argument(
    "--mode",
    choices=["all", "new", "count", "count-cache"],
    default="count-cache",
    help="""
        How to handle existing export files. 
        If 'all' will export always. 
        If 'new', will only export if the export file doesn't exist. 
        If 'count', will only export if the existing file has a different 
        number of lines. 
        If 'count-cache', same as 'count' but uses a database of line-counts
        based on ModTime and size. 
        Default: '%(default)s'
    """,
)
parser.add_argument("--dfb", help="dfb executable", default="dfb", metavar="EXE")

parser.add_argument(
    "--version",
    action="version",
    version="%(prog)s-" + __version__,
    #     help=argparse.SUPPRESS
)

group = parser.add_argument_group("log options")
group.add_argument(
    "-v",
    "--verbose",
    action="count",
    default=0,
    help="Increase verbosity (use multiple times for more detail)",
)
group.add_argument(
    "-s",
    "--silent",
    action="count",
    default=0,
    help="Decrease verbosity (use multiple times for less detail)",
)

args = parser.parse_args()

# Set log level
levels = [log.DEBUG, log.INFO, log.WARNING, log.ERROR, log.CRITICAL]
index = 1 + (args.silent - args.verbose)  # Default INFO
index = max(0, min(index, len(levels) - 1))
level = levels[index]

log.basicConfig(
    level=level,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

log.debug(f"log initialized at {log.getLevelName(level)} level")
log.debug(f"{args = }")


# Get timestamps
cmd = [args.dfb, "timestamps", "--config", args.config, "--no-header"]

log.debug(f"Executing {cmd!r}")
proc = subprocess.run(cmd, capture_output=True, text=True)

timestamps_counts = {}
for line in proc.stdout.splitlines():
    line = line.strip()
    if not line:
        continue
    line = line.split()
    time = line[0].replace(":", "")
    count = int(line[1])
    timestamps_counts[time] = count

log.info(
    f"Found {len(timestamps_counts)} timestamps and "
    f"{sum(timestamps_counts.values())} items"
)

args.dest.mkdir(parents=True, exist_ok=True)


# Helper util
def _count_lines(file, strip=True):
    """
    Count lines in `file`. If `strip` will only count non-blank lines
    """
    from dfb.utils import smart_open

    c = 0
    with smart_open(file) as fp:
        for line in fp:
            if line.strip() or not strip:
                c += 1
    return c


# Set up cache
if args.mode == "count-cache":
    try:
        from jsonlitedb import JSONLiteDB
    except ImportError:
        log.error("Must `$ pip install jsonlitedb`")
        raise

    line_count_cache = JSONLiteDB(args.dest / "line_count_cache.db")
    line_count_cache.create_index("file", unique=True)
    # I originally had the following index to speed up the mtime query but it isn't
    # worth it as the "file" query will reduce it to a single item first.
    # line_count_cache.create_index("mtime")

    def count_lines(file):
        file = Path(file)
        stat = file.stat()

        row = dict(file=str(file), size=stat.st_size, mtime=stat.st_mtime)

        dt = 0.9

        q = (stat.st_mtime - dt) <= line_count_cache.Q.mtime
        q &= line_count_cache.Q.mtime <= (stat.st_mtime + dt)
        q &= line_count_cache.Q.file == str(file)
        q &= line_count_cache.Q.size == stat.st_size

        # The idea is that if the modtime or size has changed, it will recompute and re-insert.
        # The unique index on 'file' will make it replace the old item
        res = line_count_cache.one(q)
        if not res:
            row["lines"] = _count_lines(file)
            line_count_cache.add(row, duplicates="replace")
            log.debug(f"Added or replaced DB for {str(file)!r}")
            res = row

        return res["lines"]

elif args.mode == "count":
    count_lines = _count_lines

# Process

for timestamp, count in timestamps_counts.items():
    year = timestamp[:4]
    month = timestamp[5:7]
    export_file = args.dest / year / month / f"{timestamp}.jsonl.gz"
    export_file.parent.mkdir(exist_ok=True, parents=True)

    if export_file.exists() and args.mode != "all":
        if args.mode == "new":
            log.debug(f"export {str(export_file)!r} exists. SKIP")
            continue
        # count_lines has been defined for 'count' and 'count-cache'
        if count_lines(export_file) == count:
            log.debug(f"export {str(export_file)!r} count matches. SKIP")
            continue

    cmd = [args.dfb, "snapshot", "--config", args.config, "--export"]
    cmd.extend(["--only", timestamp])
    cmd.extend(["--output", str(export_file)])

    subprocess.run(cmd, check=True)
    log.info(f"Exported {str(export_file)!r}")
