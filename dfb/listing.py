"""
CLI Listing-type functions
"""

import json
import logging
import operator
import os
import shlex
import shutil
import sys
from textwrap import dedent

from . import LOCK, time2all
from .dstdb import DFBDST
from .rclonerc import rcpathjoin
from .timestamps import timestamp_parser
from .utils import head_tail_table, human_readable_bytes, smart_open, tabulate

logger = logging.getLogger(__name__)

STRFTIME_FMT = "%Y-%m-%dT%H:%M:%S"


def snapshot(config):
    args = config.cliconfig
    dstdb = DFBDST(config)

    rows = dstdb.snapshot(
        path=args.path,
        before=args.before,
        after=args.after,
        export=args.export,  # below is ignored if export.
        remove_delete=args.deleted == 0,
        delete_only=args.deleted > 1,
        add_query="ORDER BY LOWER(apath)",
    )
    rows = (dstdb.fullrow2dict(row) for row in rows)

    if args.output:
        parent, name = os.path.split(args.output)
        swap = os.path.join(parent, f".swap.{name}")
        with smart_open(swap, "wt") as fp:
            for row in rows:
                json.dump(row, fp)
                fp.write("\n")
            fp.flush()
        shutil.move(swap, args.output)
    else:
        for row in rows:
            print(json.dumps(row, ensure_ascii=False))
        print("", end="", flush=True)


def tree(config):
    # del -- Handled in snapshot
    # del del -- Handled in snapshot
    # depth -- Done with added condition

    args = config.cliconfig
    dstdb = DFBDST(config)
    groupselect = dedent(
        """
        * 
        -- LENGTH(apath) - LENGTH(REPLACE(apath, '/', '')) AS depth -- not used
        """
    )
    path = args.path.removesuffix("/").removeprefix("./")

    conditions = []
    # Depth filtering is not done here because it means that directories aren't listed.
    # This is much more performant but is less ideal because you won't see subdirs even if
    # files exist path the depth

    # if args.max_depth > 0:
    #    d0 = len(path.split("/")) if path else 0
    #    conditions.append(["depth <= :depth", {"depth": args.max_depth - 1 + d0}])

    rows = dstdb.snapshot(
        path=path,
        before=args.before,
        after=args.after,
        groupselect=groupselect,
        conditions=conditions,
        remove_delete=args.deleted == 0,
        delete_only=args.deleted > 1,
        add_query="ORDER BY LOWER(apath)",
    )

    rows = [dstdb.fullrow2dict(row) for row in rows]

    treedict = {}
    for row in rows:
        dpath = os.path.relpath(row["apath"], path).removeprefix("./")

        parts = dpath.split("/")
        current_node = treedict
        for part in parts[:-1]:
            if part not in current_node:
                current_node[part] = {}
            current_node = current_node[part]

        # 'row' is a dict so wrap it in a tuple for later type check
        current_node[parts[-1]] = (row,)

    def _print_tree(treedict, indent="", depth=1):
        """
        Print the nested dictionary as a tree structure with line characters.
        """
        if args.max_depth > 0 and depth > args.max_depth:
            return

        # Get the keys of the dictionary
        items = list(treedict.items())

        # Iterate over each key
        for i, (key, val) in enumerate(items):
            # Determine the prefix characters based on the position in the list
            if i == len(items) - 1:
                prefix = "└── "
                next_indent = indent + "    "
            else:
                prefix = "├── "
                next_indent = indent + "│   "

            # Print the current key with the appropriate prefix
            isdir = isinstance(val, dict)
            if isdir:
                rep = f"{key}/"
            else:
                row = val[0]  # from the tuple
                rep = key
                if row["size"] < 0:
                    rep = f"{rep} (DEL)"
            print(indent + prefix + rep, flush=True)

            # If the value associated with the key is a dictionary, recursively print its contents
            if isdir:
                _print_tree(val, next_indent, depth=depth + 1)

    print(f"{path}/", flush=True)
    _print_tree(treedict)


def ls(config):
    args = config.cliconfig
    dstdb = DFBDST(config)

    subdirs, files = dstdb.ls(
        subdir=args.path,
        before=args.before,
        after=args.after,
        remove_delete=args.deleted == 0,
        delete_only=args.deleted > 1,
        recursive=args.recursive,
    )

    subdirs = list(subdirs)

    # default args.list_only option
    if args.list_only is None:
        args.list_only = "files" if args.recursive else "both"

    if args.list_only == "dirs":
        files[:] = []
    elif args.list_only == "files":
        subdirs[:] = []
    # elif args.list_only in {None, "both"}":
    #    pass # Not needed but feels more complete
    # argparse will block other options

    items = subdirs + files
    items.sort(key=lambda i: i if isinstance(i, str) else i["apath"])

    # Build a table
    table = [["versions", "total_size", "size", "ModTime", "Timestamp", "path"]]

    for item in items:
        if isinstance(item, str):  # subdir
            if (sub := os.path.relpath(item, args.path)) == ".":
                continue

            item = item if args.full_path else sub
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
                .strftime(f"{STRFTIME_FMT}")
            )

        ts = item["timestamp"]
        ts = timestamp_parser(ts)
        if args.timestamp_local:
            ts = ts.astimezone().strftime(f"{STRFTIME_FMT}%z")
        else:
            ts = ts.strftime(f"{STRFTIME_FMT}Z")

        path = item["apath"]
        if args.rpath:  # If it's a reference, we'd prefer ref_rpath if not deleted
            if item["isref"] and args.rpath == 1 and item["size"] >= 0:
                path = item["ref_rpath"]
            else:
                path = item["rpath"]
        path = path if args.full_path else os.path.relpath(path, args.path)

        if args.human:
            size = "{:0.2f} {}".format(*human_readable_bytes(item["size"]))
            tot_size = "{:0.2f} {}".format(*human_readable_bytes(item["tot_size"]))
        else:
            size = str(item["size"])
            tot_size = str(item["tot_size"])

        if item["size"] < 0:
            path = f"{path} (DEL)"
            size = "D"
        elif item["isref"]:
            size = f"{size}R"
        table.append([versions, tot_size, size, mtime, ts, path])

    if args.long == 0:
        keep = ["path"]
    elif args.long == 1:
        keep = ["size", "ModTime", "path"]
    else:
        keep = ["versions", "total_size", "size", "ModTime", "Timestamp", "path"]

    ikeep = [table[0].index(col) for col in keep]
    table = [[row[i] for i in ikeep] for row in table]

    if args.rpath:  # Rename path to rpath title
        table[0] = [c if c != "path" else "rpath" for c in table[0]]

    if not args.header:
        table = table[1:]

    if not table:
        print(f"No files under {args.path!r}. Check the path and the date")
        return

    table = head_tail_table(
        table,
        head=args.head,
        tail=args.tail,
        dots=True,
        header=args.header,
    )
    table = tabulate(table)
    print(table, flush=True)


def file_versions(config):
    args = config.cliconfig

    dstdb = DFBDST(config)
    versions = dstdb.file_versions(args.filepath, count_refs=args.ref_count)

    # Build output
    out = [f"file: {args.filepath!r}"]

    table = []
    if args.header:
        table.append(["Ref. Count", "Size", "ModTime", "Timestamp", "Real Path"])
    for item in versions:
        row = [str(item.get("ref_count", ""))]

        if args.human:
            size = "{:0.2f} {}".format(*human_readable_bytes(item["size"]))
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
                .strftime(f"{STRFTIME_FMT}")
            )
        row.append(mtime)

        ts = item["timestamp"]
        ts = timestamp_parser(ts)
        if args.timestamp_local:
            ts = ts.astimezone().strftime(f"{STRFTIME_FMT}%z")
        else:
            ts = ts.strftime(f"{STRFTIME_FMT}Z")
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
        table = head_tail_table(
            table,
            head=args.head,
            tail=args.tail,
            dots=True,
            header=args.header,
        )
        out.append(tabulate(table))
    else:
        out.append("  **No such file**. Check the path")

    out = "\n".join(out)
    print(out, flush=True)


def _timestamps_query(config):
    args = config.cliconfig
    dstdb = DFBDST(config)

    db = dstdb.db()

    before = args.before
    after = args.after
    path = args.path

    cond_dict = {}
    cond_sqls = []

    if path:
        path = path.removesuffix("/").removeprefix("./")
        cond_sqls.append("items.apath LIKE :path")
        cond_dict = {"path": f"{path}/%"}

    if before:
        b0 = before
        before = timestamp_parser(
            before,
            aware=True,
            epoch=True,
            now=config.now.obj,
        )
        logger.debug(f"Interpreted before = {b0} as {before} (s)")
        cond_sqls.append("timestamp <= :before")
        cond_dict["before"] = before

    if after:
        a0 = after
        after = timestamp_parser(
            after,
            aware=True,
            epoch=True,
            now=config.now.obj,
        )
        logger.debug(f"Interpreted after = {a0} as {after} (s)")
        cond_sqls.append("timestamp >= :after")
        cond_dict["after"] = after

    cond_sql = "WHERE " + " AND ".join(cond_sqls) if cond_sqls else ""

    # See https://stackoverflow.com/a/31704068/3633154 for the CASE WHEN ...
    ts_query = db.execute(
        f"""
        SELECT 
            timestamp,
            COUNT(timestamp) AS num_total,
            SUM(CASE WHEN size < 0 THEN 1 ELSE 0 END) AS num_del,
            SUM(CASE WHEN isref = 1 THEN 1 ELSE 0 END) AS num_mv,
            SUM(CASE WHEN (size >= 0 AND (isref IS NULL OR isref = 0) ) 
                     THEN size 
                     ELSE 0 
                     END) AS size
        FROM items 
        {cond_sql}
        GROUP BY timestamp
        ORDER BY timestamp""",
        cond_dict,
    )
    return ts_query


def timestamps(config):
    args = config.cliconfig

    ts_query = _timestamps_query(config)
    table = []
    if args.header:
        table.append(["Timestamp", "Total", "Deleted", "Moved", "Size"])

    for item in ts_query:
        timestamp = item["timestamp"]
        ts = timestamp_parser(timestamp, aware=True)

        if args.timestamp_local:
            ts = ts.astimezone().strftime(f"{STRFTIME_FMT}%z")
        else:
            ts = ts.strftime(f"{STRFTIME_FMT}Z")

        row = [ts]
        row.extend(item[k] for k in ["num_total", "num_del", "num_mv"])
        if args.human:
            size = "{:0.2f} {}".format(*human_readable_bytes(item["size"]))
        else:
            size = str(item["size"])
        row.append(size)
        table.append(row)

    table = [[str(c) for c in row] for row in table]

    table = head_tail_table(
        table,
        head=args.head,
        tail=args.tail,
        dots=True,
        header=args.header,
    )

    print(tabulate(table), flush=True)


def summary(config):
    args = config.cliconfig

    before = args.before
    after = args.after
    path = args.path

    ts = list(_timestamps_query(config))

    res = {}

    res["Path"] = repr(path.removesuffix("/").removeprefix("./") if path else "/")
    res["After"] = repr(after) if after else "<<earliest>>"
    res["Before"] = repr(before) if before else "<<latest>>"
    res["Timestamps"] = str(len(ts))

    res["Total"] = str(sum(r["num_total"] for r in ts))
    res["Deleted"] = str(sum(r["num_del"] for r in ts))
    res["Moved"] = str(sum(r["num_mv"] for r in ts))

    res["Size"] = sum(r["size"] for r in ts)
    res["Size"] = (
        f"{res['Size']} ({human_readable_bytes(res['Size'],fmt=True,short=True)})"
    )

    print(tabulate([[f"{k}:", v] for k, v in res.items()]))


def timestamp_include_filters(config):
    args = config.cliconfig
    dstdb = DFBDST(config)
    db = dstdb.db()

    before = args.before
    after = args.after
    path = args.path

    conditions = []

    if path:
        path = path.removesuffix("/").removeprefix("./")
        conditions.append(("items.apath LIKE :path", {"path": f"{path}/%"}))

    if before:
        b0 = before
        before = timestamp_parser(
            before,
            aware=True,
            epoch=True,
            now=config.now.obj,
        )
        logger.debug(f"Interpreted before = {b0} as {before} (s)")
        conditions.append(("timestamp <= :before", {"before": before}))

    if after:
        a0 = after
        after = timestamp_parser(
            after,
            aware=True,
            epoch=True,
            now=config.now.obj,
        )
        logger.debug(f"Interpreted after = {a0} as {after} (s)")
        conditions.append(("timestamp >= :after", {"after": after}))

    cond = ""
    params = {}
    if conditions:
        cond = "WHERE " + " AND ".join(condition[0] for condition in conditions)
        params |= {k: v for condition in conditions for k, v in condition[1].items()}

    query = f"""
        SELECT DISTINCT timestamp
        FROM items
        {cond}
        ORDER BY timestamp
        """

    qres = db.execute(query, params)
    includes = []
    for item in qres:
        timestamp = item["timestamp"]
        ts = timestamp_parser(timestamp, aware=True)
        dt = time2all(ts).dt

        includes.extend(["--include", f"*.{dt}*"])  # may or may not have a dot after

    print(shlex.join(includes))
