"""
Database of the destination. Includes the tools and methods to refresh it.
"""

import os
from pathlib import Path
import sqlite3
import time
import io
import json
import logging
import string
import shutil
import gzip as gz
from functools import partialmethod
from textwrap import dedent, indent

from . import __version__, nowfun, rpath2apath, apath2rpath
from .utils import (
    time2all,
    MyRow,
    star,
    listify,
    smart_open,
    randstr,
    smart_splitext,
    NoTimestampInNameError,
)
from .timestamps import timestamp_parser
from . import rclonerc
from .rclonerc import IGNORED_FILE_DATA, rcpathjoin
from .threadmapper import thread_map_unordered as tmap


logger = logging.getLogger(__name__)


def sqldebug(sql):
    return
    sql = "\n".join(line for line in sql.split("\n") if line.strip())
    sql = dedent(sql).rstrip()
    logger.info(f"SQL >>>>>>>>>>>>>>> DSTDB\n{sql}\n<<<<<<<<<<<<<<<")


class DFBDST:
    """
    Main database object for the destination
    """

    COLS = (
        ("rpath", "TEXT NOT NULL"),  # Full path to the real file
        ("apath", "TEXT NOT NULL"),  # Full path to aparent name
        ("timestamp", "INTEGER NOT NULL"),
        ("size", "INTEGER"),
        ("mtime", "REAL"),
        ("checksum", "TEXT"),
        ("isref", "INTEGER"),  # 0: not ref, 1: ref, 2: ref not updated
        ("ref_rpath", "TEXT"),
        ("dstinfo", "INTEGER"),  # Information is from the dest, not source
        ("remain", "TEXT"),
    )

    def __init__(self, config):
        self.config = config
        self.dst_rclone = dst_rclone = config._config["dst_rclone"]
        self.dbcache_dir = config.dbcache_dir

        self.snap_file = (
            self.config.snap_cache_dir
            / self.config.now.obj.strftime("%Y/%m")
            / f"{self.config.now.dt}Z.jsonl"
        )
        self.snap_file.parent.mkdir(exist_ok=True, parents=True)

        dbpath = self.dbcache_dir / f"{config.config_id}.db"
        dbpath.parent.mkdir(exist_ok=True, parents=True)

        self.dbpath = dbpath

        self.init()

    def db(self):
        db = sqlite3.connect(self.dbpath, check_same_thread=True)
        db.row_factory = MyRow
        db.set_trace_callback(sqldebug)
        return db

    def init(self):
        # We will only write to the DB in the main thread but will
        # read in many
        items = ",".join((" ".join(row)) for row in self.COLS)
        db = self.db()

        try:
            with db:
                db.execute("PRAGMA journal_mode = wal")
        except sqlite3.OperationalError:  # pragma: no cover
            pass

        # test:
        try:
            with db:
                r = db.execute(
                    """
                    SELECT * FROM kv 
                    WHERE key = ? OR key = ?
                    ORDER BY key""",
                    ("created", "version"),
                ).fetchall()
                if len(r) == 2:  # Note it is ORDER BY so the order wont change
                    created, version = [i["val"] for i in r]
                    logger.debug(f"dstdb exists. {created = } {version = }")
                    return
        except:
            logger.debug("Recreate dstdb")

        with db:
            db.execute(
                f"""
                CREATE TABLE IF NOT EXISTS
                items(
                    {items},
                    PRIMARY KEY (apath, timestamp)
                )"""
            )

            db.execute(
                """
                CREATE TABLE IF NOT EXISTS kv(
                    key TEXT PRIMARY KEY,
                    val BLOB
                )"""
            )

            db.execute(
                """
                INSERT OR IGNORE INTO kv VALUES (?,?)
                """,
                ("created", self.config.now.obj.isoformat()),
            )
            db.execute(
                """
                INSERT OR IGNORE INTO kv VALUES (?,?)
                """,
                ("version", __version__),
            )
        db.commit()
        db.close()

    def reset(self, stats=None, *, use_snapshots):
        if self.config.disable_refresh:
            logger.error("Refresh not allowed due to 'disable_refresh = True")
            logger.error("Run with `--override 'disable_refresh = False'` to override")
            raise ValueError("Refresh Disabled")

        self.dbpath.unlink()
        self.init()

        files = list(self._relist(stats=stats))  # Need them all to get snapshots
        logger.info(
            f"Found {len(files)} at dest "
            f"with {sum(f.get('isref',0) == 2 for f in files)} reference(s)"
        )

        if use_snapshots:
            self._load_snapshots()
            files = (self._apply_snapshot_file(file) for file in files)

        db = self.db()
        for ii, file in enumerate(files):
            db.execute(
                f"INSERT INTO items VALUES ({','.join('?' for _ in DFBDST.COLS)})",
                DFBDST.dict2fullrow(file),
            )
            if ii % 100 == 0:  # Frequent commits but not every time.
                db.commit()
        db.commit()
        db.close()

        # Update those with isref = 2. Do this after full listing
        self._update_references()

    def _relist(self, stats=None):
        self._snapshot_list = []

        config = self.config
        flags = config.dst_list_rclone_flags

        files = config.dst_rclone.listremote(
            mimetype=False,
            # Notice modtime and hashes are only if needed and not using get_modtime
            # or get_hashes
            modtime="mtime" in (config.dst_compare, config.dst_renames),
            hashes="hash" in (config.dst_compare, config.dst_renames),
            hashtypes=config.hash_type,
            # metadata=... # Set in universal_flags#
            only="files",
            epoch_time=True,
            flags=flags,  # Will include fast-list if needed
            #             pipe=False,
            filters=["- **/.swap.*", "- /.dfb/**"],
        )

        t0 = time.time()
        c = 0
        for file in files:
            try:
                apath, ts, flag = rpath2apath(file["Path"])
            except (ValueError, NoTimestampInNameError, IndexError):
                logger.error(f"Could not find timestamp for {file['Path']}. Ignoring")
                continue
            c += 1

            size = file.pop("Size")
            new = {
                "rpath": file.pop("Path"),
                "apath": apath,
                "timestamp": ts,
                "size": size if flag != "D" else -1,
                "mtime": file.pop("ModTime", None),
                "isref": 2 if flag == "R" else 0,  # 2 means not yet updated. Later
                "dstinfo": True,
            }
            if hashes := file.pop("Hashes", None):
                new["checksum"] = hashes

            # Update with everything else
            for k, v in file.items():
                if k in IGNORED_FILE_DATA:
                    continue
                new[k] = v

            if stats and (time.time() - t0) >= stats:  # TODO TEST
                logger.info(f"Destination Listing Status: {c} items")
                t0 = time.time()

            yield new

    def _update_references(self):
        db = self.db()
        with db:
            files = db.execute("""SELECT * FROM items WHERE isref = 2""")
            files = files.fetchall()

        if not files:
            return

        logger.info(f"Need to fetch {len(files)} references")

        # Multi-thread reading from the remote to get the new rpath
        # and reading from the DB to get the info
        rc = self.config.rc
        rc.start()

        def _get_referent(file):
            refferer = file["rpath"]
            referent = rc.read((self.config.dst, refferer)).decode()

            # Handle different versions here
            try:
                referent = json.loads(referent)
            except json.JSONDecodeError:
                logger.debug(f"Reading reference. Assuming V1")
                referent = {"ver": 1, "path": referent}

            ver = referent["ver"]
            if ver == 1:
                logger.debug(f"Reference {refferer!r} is v1 (implied)")
                return file, referent["path"]
            elif ver == 2:
                logger.debug(f"Reference {refferer!r} is v2")
                path = os.path.join(os.path.dirname(refferer), referent["rel"])
                path = os.path.normpath(path)
                return file, path
            raise ValueError("Unrecognized Version")

        files = tmap(_get_referent, files, Nt=1)  # self.config.concurrency)

        def _update(file, referent):
            referent = referent.strip("\n")
            refferer = file["rpath"]
            # Get the original information
            row = db.execute(
                """
                SELECT * FROM items 
                WHERE rpath = ? AND NOT isref""",
                (referent,),
            ).fetchone()

            if not row:
                txt = f"WARNING: File {refferer!r} references {referent!r} "
                txt += "but it is missing. Will just be treated as deleted"
                logger.warning(txt)
                row = DFBDST.fullrow2dict(file)
                row["size"] = -1
                return row

            row = DFBDST.fullrow2dict(row)
            row.pop("Size", None)
            # Reset some values
            row["apath"] = file["apath"]
            row["timestamp"] = file["timestamp"]
            row["isref"] = 1  # Resolved reference
            row["ref_rpath"] = refferer
            return row

        files = map(star(_update), files)

        # Insert into DB in the main thread.
        for ii, file in enumerate(files):
            db.execute(
                f"REPLACE INTO items VALUES ({','.join('?' for _ in DFBDST.COLS)})",
                DFBDST.dict2fullrow(file),
            )
            logger.info(f"updated reference {file['ref_rpath']!r}")
            # Fetching references is slow so commit more often to make sure we don't
            # lose much. Plus, we are waiting for updates
            if ii % 10 == 0:
                db.commit()
        db.commit()
        db.close()

    def _load_snapshots(self):
        # May have to rethink this for memory but that would be a *lot* of files
        self._snapshot_dict = {}
        self._snapshot_dict_ref = {}

        logger.debug("Loading snapshots from remote")

        rc = self.config.rc
        rc.start()

        # Do a sync all at once so it can be threaded with rclone directly
        params = {}
        params["_config"] = {
            "SizeOnly": True,  # These files shouldn't change. No need to worry about mtime
            "UseListR": True,  # Not too many files and could be much better
            "Transfers": self.config.concurrency,
        }

        params["srcFs"] = rcpathjoin(self.config.dst, ".dfb/snapshots")
        snap_dest = self.dbcache_dir / self.config.config_id
        params["dstFs"] = str(snap_dest)
        try:
            logger.debug(f"sync snaps with {params = }")
            rc.call("sync/sync", params=params)
        except rclonerc.RcloneError:
            logger.info("Unable to load snapshots from remote to accelerate refresh.")
            logger.debug(f"Couldn't load {params['srcFs']!r}")
            return

        for snap in sorted(snap_dest.rglob("**/*.jsonl*"), key=lambda p: p.name):
            c = 0
            for line in smart_open(str(snap)):
                line = json.loads(line)
                if (
                    line.get("_action", None) in {"prune", "comment"}
                    or line["size"] < 0
                ):
                    continue

                if line.get("isref", False):
                    self._snapshot_dict_ref[line["ref_rpath"]] = line
                else:
                    self._snapshot_dict[line["rpath"]] = line

                c += 1
            logger.info(f"Loaded {c} items from {snap.name}")

    def _apply_snapshot_file(self, file):
        if file.get("isref", False) == 2:
            if snapfile := self._snapshot_dict_ref.get(file["rpath"]):
                logger.info(
                    f"Updated reference for {file['rpath']!r} "
                    f"from {snapfile['rpath']!r}"
                )
                keep = {
                    "apath": file["apath"],
                    "timestamp": file["timestamp"],
                    "ref_rpath": file["rpath"],
                    "isref": 1,
                }

                file.clear()
                file |= snapfile | keep

            return file
        # ONLY apply if the file is listed already. Otherwise, see dbimport. This
        # includes ignore prune entries
        if not (snapfile := self._snapshot_dict.get(file["rpath"], None)):
            return file

        # They should be the same by rpath but just do a quick sanity check.
        # Include checking the timestamp for some odd files

        if (
            file["size"] != snapfile["size"]
            or file["timestamp"] != snapfile["timestamp"]
        ):
            logger.warning(
                f"snapshot for rpath = {file['rpath']!r} does not match "
                "as expected. Ignoring"
            )
            return file

        # Update and reset the file
        file |= snapfile
        file["dstinfo"] = False
        return file

    def dbimport(self, exportfiles, exportdirs, reset=False, upload=False):
        if self.config.disable_refresh:
            logger.error("DB Import not allowed due to 'disable_refresh = True")
            logger.error("Run with `--override 'disable_refresh = False'` to override")
            raise ValueError("Refresh Disabled")

        if reset:
            self.dbpath.unlink()
            self.init()

        rc = self.config.rc.start()

        # Parallelize downloading all export files. Download the specified ones and sync
        # the directories. Will then walk the file system and load them all. Note that
        # order doesn't matter for entries except prunes. So will load all entries in
        # whatever final order and then do prunes. Use random names so that there isn't
        # a conflict with the same filename

        imp = self.config.tmpdir / "import" / randstr(8)
        imp.mkdir(exist_ok=False, parents=True)

        def _dl_exportfile(exportfile):
            logger.debug(f"Downloading file {exportfile!r}")
            rc.copyfile(
                src=exportfile,
                dst=(str(imp), f"{randstr(6)}/{os.path.basename(exportfile)}"),
                _config={"NoCheckDest": True},
            )

        for _ in tmap(_dl_exportfile, exportfiles, Nt=self.config.concurrency):
            pass

        # Do these serially since parallel will be done later
        for exportdir in exportdirs:
            logger.debug(f"Downloading directory {exportdir!r}")
            params = {}
            params["_config"] = {
                "IgnoreTimes": True,  # Always download
                "UseListR": True,  # Not too many files and could be much better
                "Transfers": self.config.concurrency,
            }

            params["srcFs"] = exportdir
            params["dstFs"] = rcpathjoin(str(imp), randstr(6))
            rc.call("sync/sync", params=params)

        # The "sorted" is not really needed but it is likely to keep the
        # database cleaner
        loadfiles = sorted(imp.rglob("*.jsonl*"), key=lambda file: file.name)

        prune = []  # prune is OUTSIDE the file loop as noted above
        for exportfile in loadfiles:
            logger.debug(f"Importing from {str(exportfile)!r}")
            files = []
            pcount = 0
            with smart_open(exportfile, "rt") as fp:
                for line in fp:
                    file = json.loads(line)
                    if file.get("_action", None) == "comment":
                        continue

                    if file.get("_action", None) == "prune":
                        prune.append(file)
                        pcount += 1
                        continue

                    files.append(file)

            with self.db() as db:
                db.executemany(
                    f"""INSERT OR REPLACE INTO items 
                        VALUES ({','.join('?' for _ in DFBDST.COLS)})""",
                    [DFBDST.dict2fullrow(file) for file in files],
                )
            msg = f"  Imported {len(files)} files"
            if pcount:
                msg += f" and will prune {pcount}"
            logger.info(msg)

        if prune:
            with self.db() as db:
                db.executemany(
                    """
                    DELETE FROM items 
                    WHERE rpath = ?""",
                    ((file["rpath"],) for file in prune),
                )
            logger.info(f"Pruned {len(prune)} files from all exports")

        if upload:
            snapdir = self.snap_file.parent / self.snap_file.stem
            snapdir.mkdir(parents=True)  # Shouldn't already exists

            for ii, loadfile in enumerate(loadfiles):
                dst = snapdir / f"{ii}.{loadfile.name}"
                shutil.copy2(loadfile, dst)

            self.push_snapshots(compress=False)

    def insert_or_replace_many(self, files, *, insert, replace):
        """
        Allows inserting or replacing. This requires being explicit to avoid wrong
        insertions
        """
        action = []
        if insert:
            action.append("INSERT")
        if replace:
            action.append("REPLACE")
        action = " OR ".join(action)
        sql = f"{action} INTO items VALUES ({','.join('?' for _ in DFBDST.COLS)})"

        # Collect them all. We will do it anyway in the DB and this way it can be yielded
        files = list(files)
        # Insert into DB in the main thread
        rows = map(DFBDST.dict2fullrow, files)
        # ALWAYS wait before an executemany since that could lock the DB
        rows = list(rows)

        db = self.db()
        with db:
            db.executemany(sql, rows)
        # db.commit()
        # db.close()

        with self.snap_file.open(mode="at") as fp:
            for file in files:
                print(json.dumps(file), file=fp, flush=True)

        return files

    insert_many = partialmethod(insert_or_replace_many, insert=True, replace=False)
    replace_many = partialmethod(insert_or_replace_many, insert=False, replace=True)

    def _insert_or_replace(self, file, *, insert, replace):
        """
        Allows inserting or replacing. This requires being explicit to avoid wrong
        insertions.

        These are also partialmethods defined below

            Insert only: db._insert_or_replace(file,insert=True,replace=False)
            Insert only: db._insert_or_replace(file,insert=False,replace=True)
            both:      : db._insert_or_replace(file,insert=True,replace=True)

        """
        return self.insert_or_replace_many([file], insert=insert, replace=replace)

    insert = partialmethod(_insert_or_replace, insert=True, replace=False)
    replace = partialmethod(_insert_or_replace, insert=False, replace=True)
    insert_or_replace = partialmethod(_insert_or_replace, insert=True, replace=True)

    def _snapshot_query_builder(
        self,
        *,
        path="",
        before=None,
        after=None,
        select="*",
        groupselect="*",
        export=False,
        remove_delete=True,
        delete_only=False,
        conditions=None,
        query_prefix="snap",
    ):
        """
        Build a query for snapshots. This can then be evaluated later directory or as a
        subquery.

        Inputs:
        -------

        path: ''
            Starting path

        before:
            Select files <= before. This is the "at" snapshot time. Will be parsed by
            timestamp_parser. Times are inclusive on both ends

        after:
            Select files >= after. This is the "at" snapshot time. Will be parsed by
            timestamp_parser. Times are inclusive on both ends

        select
            What to return. Used after the GROUP BY statement

        groupselect
            Select inside the GROUP BY statement. Useful for additional values
            but MUST also include "*"

        export [False]
            If True, includes multiples. Will override remove_delete and delete_only

        remove_delete: [True]
            If False, will keep deleted items. Uses a subquery which should be faster
            than manual filtering. If used with delete_only, will get nothing.

        delete_only [False]
            Only show deleted items. If used with remove_delete, will get nothing

        conditions:
            List of additional (sql,param_dict) pairs. Warning: Do not let sql be user input.
            Examples: ('apparentparent LIKE :path',{'path':'a/sub/path/'})

            WARNING: Do not do ('size >= ?',0) since that will then include the non-deleted
                     version. It is better to filter it later.

        query_prefix: Prefix to be used for all query parameters. Can be useful if building
                 subqueries

        Returns:
        --------
        query : Text of the query
        params: Dictionary of the parameter substitutions

        """
        qp = query_prefix

        conditions = conditions or []
        if "*" not in groupselect:
            raise ValueError("groupselect must have '*'")

        query = []
        params = {}

        if export:
            remove_delete = delete_only = False

        # Build the snapshot. Note that the select is never *user*
        # specified so there isn't an SQL injection risk.

        # Always build as a nested query even if not needed. The query optimizer
        # should flatten it. This makes the dynamic construction much easier.
        # From ChatGPT:
        #    While the performance impact of using SELECT * FROM (SELECT * FROM items)
        #    is generally negligible in SQLite due to query optimization, it's a good
        #    practice to avoid such constructs unless they serve a specific purpose,
        #    such as when dynamically building complex queries.

        query.extend(
            [
                "SELECT",
                groupselect,
                "FROM items",
            ]
        )

        if path:
            path = path.removesuffix("/").removeprefix("./")
            conditions.append((f"apath LIKE :{qp}_path", {f"{qp}_path": f"{path}/%"}))

        if before:
            b0 = before
            before = timestamp_parser(
                before,
                aware=True,
                epoch=True,
                now=self.config.now.obj,
            )
            logger.debug(f"Interpreted before = {b0} as {before} (s)")
            conditions.append((f"timestamp <= :{qp}_before", {f"{qp}_before": before}))

        if after:
            a0 = after
            after = timestamp_parser(
                after,
                aware=True,
                epoch=True,
                now=self.config.now.obj,
            )
            logger.debug(f"Interpreted after = {a0} as {after} (s)")
            conditions.append((f"timestamp >= :{qp}_after", {f"{qp}_after": after}))

        if conditions:
            query.append("WHERE")
            query.append(" AND ".join(cond[0] for cond in conditions))
            params |= {k: v for c in conditions for k, v in c[1].items()}

        if not export:
            query.append("GROUP BY apath HAVING MAX(timestamp)")
        # query.append("ORDER BY LOWER(apath)")

        query = "\n".join(query)

        outq_cond = []
        if remove_delete:
            outq_cond.append("size >= 0")
        if delete_only:
            outq_cond.append("size < 0")

        # Build the nested query even if not needed to capture the different
        # select and groupselect
        query = "-- Subquery with everything that is then filtered\n" + query
        query = f"SELECT {select} FROM (\n{indent(query,' '*4)}\n)"
        if outq_cond:
            query += "\nWHERE\n" + " AND ".join(outq_cond)

        return query, params

    def snapshot(self, add_query="", **kwargs):
        """
        See _snapshot_query_builder for help. Can specify additional queries but must be
        in select.
        """
        query, params = self._snapshot_query_builder(**kwargs)
        if add_query:
            query += "\n" + add_query

        db = self.db()
        with db:
            r = db.execute(query, params)
        return r

    def ls(
        self,
        subdir="",
        *,
        before=None,
        after=None,
        select="*",
        remove_delete=True,
        delete_only=False,
        conditions=None,
        recursive=False,
    ):
        """

        Build a query.

        path: ''
            Starting path

        before:
            Select files <= before. This is the "at" snapshot time. Will be parsed by
            timestamp_parser. Times are inclusive on both ends

        after:
            Select files >= after. This is the "at" snapshot time. Will be parsed by
            timestamp_parser. Times are inclusive on both ends

        select
            What to return.

        remove_delete: [True]
            If False, will keep deleted items. Uses a subquery which should be faster
            than manual filtering. If used with delete_only, will get nothing.

        delete_only [False]
            Only show deleted items. If used with remove_delete, will get nothing

        conditions:
            List of additional (sql,val) pairs. Warning: Do not let sql be user input.
            Examples: ('apparentparent LIKE ?','a/sub/path/')

            WARNING: Do not do ('size >= ?',0) since that will then include the non-deleted
                     version. It is better to filter it later.

        recursive: [False]
            List all items, not just the one directory

        Some of this very clever SQL came from my reddit post here:
        https://www.reddit.com/r/sqlite/comments/123bivr/comment/jdu9xvl/?context=3
        """
        subdir = subdir.removeprefix("./").removesuffix("/")
        db = self.db()

        # This method makes essentially two queries that look a lot like snapshots
        #     1. Snapshot like query that then filters using the above-noted clever SQL
        #        to filter subdirs. (The old version of this did something similar w/o
        #        filters for the latest then checked each dir. This is more efficient)
        #
        #     2. Files: Add conditions to apath to make sure it only lists in that
        #        parent directory

        conditions = conditions or []

        ## Files
        fcond = conditions.copy()

        if not recursive:
            fcond.append(
                [
                    "apath NOT LIKE :onedepth",
                    {"onedepth": os.path.join(subdir, "%", "%")},
                ]
            )

        groupselect = dedent(
            """
            *, 
            COUNT(*) AS versions,
            SUM(
                CASE  
                    WHEN size > 0 THEN size 
                    ELSE 0
                END
            ) as tot_size -- Need to account for -1 vals
            """
        )

        fquery, fparams = self._snapshot_query_builder(
            path=subdir,
            before=before,
            after=after,
            select="*",
            groupselect=groupselect,
            remove_delete=remove_delete,
            delete_only=delete_only,
            conditions=fcond,
        )

        files = [DFBDST.fullrow2dict(r) for r in db.execute(fquery, fparams)]

        ## Directories.
        if recursive:
            # Do this in Python as it is cleaner than SQL
            directories = {os.path.dirname(file["apath"]) for file in files}

            for directory in directories.copy():
                if not directory:  # At root
                    continue

                directory = os.path.relpath(directory, subdir)  # remove subdir
                while directory:
                    if directory := os.path.dirname(directory):
                        directories.add(os.path.join(subdir, directory))

            directories.difference_update({"", "./"})
        else:
            # Use the snapshot query builder with all of the conditions to make a query with
            # all valid files. Then use the fancy SQL to down-select directories. If it is a
            # subdir, it needs an additional filter to remove the subdir from the apath names.
            # The "QQQQ" sub is purely cosmetic to get the indents of the subquery *after* the
            # dedents of the outer query
            dir_query, dir_params = self._snapshot_query_builder(
                path=subdir,
                before=before,
                after=after,
                select="apath",
                remove_delete=remove_delete,
                delete_only=delete_only,
                conditions=conditions,
            )

            params = dir_params.copy()
            if subdir:
                query = dedent(
                    f"""
                    WITH
                        snappaths AS (
                        QQQQ
                        ),
                        subpaths AS (
                            SELECT SUBSTR(snappaths.apath, {len(subdir) + 2}) AS apath
                            FROM snappaths
                            WHERE snappaths.apath LIKE :subdir
                        )
                        
                    """
                ).replace("QQQQ", indent(dir_query, " " * 8))
                params["subdir"] = f"{subdir}/%"
            else:
                query = dedent(
                    f"""
                    WITH
                        subpaths AS (
                        QQQQ
                        )"""
                ).replace("QQQQ", indent(dir_query, " " * 8))

            query += dedent(
                """
                -- Get just the next path element
                -- https://www.reddit.com/r/sqlite/comments/123bivr/comment/jdu9xvl/?context=3
                SELECT DISTINCT 
                        SUBSTR(
                            apath,
                            1,
                            CASE INSTR(apath, '/')
                                WHEN 0
                                THEN LENGTH(apath)
                                ELSE INSTR(apath, '/')
                            END
                        ) AS sub
                FROM subpaths
                """
            )
            apaths = (r["sub"] for r in db.execute(query, params))
            apaths = (apath for apath in apaths if apath.endswith("/"))
            directories = [os.path.join(subdir, apath) for apath in apaths]

        return directories, files

    def file_versions(self, filepath, count_refs=False):
        db = self.db()
        with db:
            versions = db.execute(
                "SELECT * FROM items WHERE apath = ? ORDER BY timestamp", (filepath,)
            )
        versions = [self.fullrow2dict(v) for v in versions]

        if count_refs:
            for file in versions:
                counts = db.execute(
                    """
                    SELECT COUNT(rpath) AS count FROM items
                    WHERE rpath = ?""",
                    (file["rpath"],),
                ).fetchone()
                file["ref_count"] = counts.get("count", default=0)

        db.close()
        return versions

    def group_by_apath(self, select="*", conditions=None):
        """
        Group by apath where each group will be sorted by timestamp.
        (so you can use bisect to quickly find elements)

        Inputs:
        ------

        select
            What to return. MUST include 'apath'

        conditions:
            NOTE: different than in ls
            query_txt, qvals such as
                query_txt = "WHERE size > ? and size < ?"
                qvals = [100,200]


        """
        db = self.db()

        if conditions:
            qtxt, qvals = conditions
            qvals = listify(qvals)
        else:
            qtxt, qvals = "", []

        with db:
            Qres = db.execute(
                f"""
                SELECT {select} FROM items
                {qtxt}
                ORDER BY
                    LOWER(apath),timestamp""",
                qvals,
            )
            Qres = map(DFBDST.fullrow2dict, Qres)

        row = next(Qres)
        try:
            name = row["apath"]
        except KeyError:
            raise ValueError("Must include 'apath' in 'select'")
        group = [row]

        for row in Qres:
            if row["apath"] == name:
                group.append(row)
            else:
                yield name, group
                group = [row]
                name = row["apath"]
        yield name, group  # Last item

    def push_snapshots(self, compress=True):
        """
        Compress (optional) and push all snapshots.
        This will catch ones from interrupted runs
        """
        for snap_file in self.config.snap_cache_dir.rglob("**/*.jsonl"):
            if snap_file.stat().st_size == 0:
                logger.debug(f"Empty snapshot {str(snap_file)!r}. Unlink")
                snap_file.unlink()
            elif compress:
                snapz = snap_file.with_suffix(".jsonl.gz")
                with gz.open(str(snapz), "wb") as fz, snap_file.open("rb") as fu:
                    shutil.copyfileobj(fu, fz)
                snap_file.unlink()
                logger.debug(f"Compressed {str(snap_file)!r} to {str(snapz)!r}")

        rc = self.config.rc
        rc.start()

        params = {
            "createEmptySrcDirs": False,
            "deleteEmptySrcDirs": True,  # Cleans local
        }
        params["_config"] = {
            "NoCheckDest": True,  # We know they shouldn't exists
            "NoTraverse": True,
            "NoUpdateDirModTime": True,  # Don't care
            "Transfers": self.config.concurrency,
            "Metadata": self.config.metadata,
        }
        params["srcFs"] = str(self.config.snap_cache_dir)
        params["dstFs"] = rcpathjoin(self.config.dst, ".dfb/snapshots")

        logger.debug(f"moving files from {params['srcFs']!r} to {params['dstFs']!r}")
        rc.call("sync/move", params=params)

    @classmethod
    def dict2fullrow(cls, rowdict):
        """Take a dict of the file and convert it to a DB row"""
        rowdict = rowdict.copy()

        if cs := rowdict.get("checksum", None):
            rowdict["checksum"] = json.dumps(cs)

        row = [rowdict.pop(key, None) for key, _ in cls.COLS[:-1]]
        row.append(json.dumps(rowdict) if rowdict else None)  # remain
        return row

    @staticmethod
    def fullrow2dict(row):
        row = dict(row)

        try:
            row["checksum"] = json.loads(row["checksum"])
        except (KeyError, TypeError, json.JSONDecodeError):
            pass

        if remain := row.pop("remain", None):
            row.update(json.loads(remain))

        return row
