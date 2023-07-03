"""
CLI Listing-type functions
"""
import os, sys
import shutil
import json
import operator

from . import LOCK, log, debug
from .dstdb import DFBDST
from .utils import tabulate, bytes2human
from .timestamps import timestamp_parser
from .rclone import rcpathjoin

_r = repr

# Module CLIlisting


def snapshot(config):
    args = config.cliconfig
    dstdb = DFBDST(config)

    rows = dstdb.snapshot(
        path=args.path,
        before=args.before,
        after=args.after,
        remove_delete=args.deleted == 0,
        delete_only=args.deleted > 1,
    )
    rows = (dstdb.fullrow2dict(row) for row in rows)

    if args.output:
        swap = args.output + ".swp"
        with open(swap, "wt") as fp:
            for row in rows:
                json.dump(row, fp)
                fp.write("\n")
            fp.flush()
        shutil.move(swap, args.output)
    else:
        for row in rows:
            log.print(json.dumps(row, ensure_ascii=False))
        log.print("", end="", flush=True)


def ls(config):
    args = config.cliconfig
    dstdb = DFBDST(config)

    subdirs, files = dstdb.ls(
        subdir=args.path,
        before=args.before,
        after=args.after,
        remove_delete=args.deleted == 0,
        delete_only=args.deleted > 1,
    )
    ####
    items = list(subdirs) + files
    items.sort(key=lambda i: i if isinstance(i, str) else i["apath"])

    # Build a table
    table = []
    if not args.no_header:
        table.append(["versions", "total_size", "size", "ModTime", "Timestamp", "path"])
    for item in items:
        if isinstance(item, str):  # subdir
            item = item if args.full_path else os.path.relpath(item, args.path)
            table.append(["", "", "", "", "", f"{item.removesuffix('/')}/"])
            continue

        versions = str(item["versions"])

        mtime = item.get("mtime", None)
        if not mtime:
            mtime = ""
        else:
            mtime = (
                timestamp_parser(mtime, aware=True)
                .astimezone()
                .strftime("%Y-%m-%d %H:%M:%S")
            )

        ts = item["timestamp"]
        ts = timestamp_parser(ts)
        if args.timestamp_local:
            ts = ts.astimezone().strftime("%Y-%m-%d %H:%M:%S%z")
        else:
            ts = ts.strftime("%Y-%m-%d %H:%M:%SZ")
        path = item["apath"]
        path = path if args.full_path else os.path.relpath(path, args.path)

        if args.human:
            size = "{:0.2f} {}".format(*bytes2human(item["size"]))
            tot_size = "{:0.2f} {}".format(*bytes2human(item["tot_size"]))
        else:
            size = str(item["size"])
            tot_size = str(item["tot_size"])

        if item["size"] < 0:
            path = f"{path} (DEL)"
            size = "D"
        table.append([versions, tot_size, size, mtime, ts, path])

    if args.long == 0:
        table = [row[-1:] for row in table]
    elif args.long == 1:
        table = [[row[2], row[3], row[5]] for row in table]  # size,ModTime,path
    else:  # args.long == 2:
        pass  # Just to be more clear

    if not table:
        log.print(f"No files under {_r(args.path)}. Check the path and the date")
        return

    table = tabulate(table)
    log.print(table, flush=True)


def file_versions(config):
    args = config.cliconfig

    dstdb = DFBDST(config)
    versions = dstdb.file_versions(args.filepath, count_refs=args.ref_count)

    # Build output
    out = [f"file: {_r(args.filepath)}"]

    table = []
    if not args.no_header:
        table.append(["Ref. Count", "Size", "ModTime", "Timestamp", "Real Path"])
    for item in versions:
        row = [str(item.get("ref_count", ""))]

        if args.human:
            size = "{:0.2f} {}".format(*bytes2human(item["size"]))
        else:
            size = str(item["size"])

        if item["size"] < 0:
            size = f"D"
        if item.get("isref", False):
            size = f"{size} (R)"
        row.append(size)

        mtime = item.get("mtime", None)
        if not mtime:
            mtime = ""
        else:
            mtime = (
                timestamp_parser(mtime, aware=True)
                .astimezone()
                .strftime("%Y-%m-%d %H:%M:%S")
            )
        row.append(mtime)

        ts = item["timestamp"]
        ts = timestamp_parser(ts)
        if args.timestamp_local:
            ts = ts.astimezone().strftime("%Y-%m-%d %H:%M:%S%z")
        else:
            ts = ts.strftime("%Y-%m-%d %H:%M:%SZ")
        row.append(ts)

        if args.real_path >= 2:
            row.append(rcpathjoin(config.dst, item["rpath"]))
        else:
            row.append(item["rpath"])

        table.append(row)

    if not args.ref_count:
        table = [row[1:] for row in table]
    if not args.real_path:
        table = [row[:-1] for row in table]

    if table:
        out.append(tabulate(table))
    else:
        out.append("  **No such file**. Check the path")

    out = "\n".join(out)
    log.print(out, flush=True)


def timestamps(config):
    args = config.cliconfig
    dstdb = DFBDST(config)

    db = dstdb.db()
    # See https://stackoverflow.com/a/31704068/3633154 for the CASE WHEN ...
    snapshots = db.execute(
        """
        SELECT 
            timestamp,
            COUNT(timestamp) AS num_total,
            SUM(CASE WHEN size < 0 THEN 1 ELSE 0 END) AS num_del,
            SUM(CASE WHEN isref = 1 THEN 1 ELSE 0 END) AS num_mv,
            SUM(CASE WHEN (size >= 0 
                      AND (isref IS NULL OR isref = 0) ) 
                      THEN size ELSE 0 END) AS size
        FROM items 
        GROUP BY timestamp
        ORDER BY timestamp"""
    )

    table = []
    if not args.no_header:
        table.append(["Timestamp", "Total", "Deleted", "Moved", "Size"])

    for item in snapshots:
        timestamp = item["timestamp"]
        ts = timestamp_parser(timestamp, aware=True)

        if args.timestamp_local:
            ts = ts.astimezone().strftime("%Y-%m-%d %H:%M:%S%z")
        else:
            ts = ts.strftime("%Y-%m-%d %H:%M:%SZ")

        row = [ts]
        row.extend(item[k] for k in ["num_total", "num_del", "num_mv"])
        if args.human:
            size = "{:0.2f} {}".format(*bytes2human(item["size"]))
        else:
            size = str(item["size"])
        row.append(size)
        table.append(row)

    table = [[str(c) for c in row] for row in table]
    log.print(tabulate(table))
