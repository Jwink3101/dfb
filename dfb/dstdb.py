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
from functools import partialmethod
from textwrap import dedent, indent

from . import __version__, nowfun
from .utils import time2all, MyRow, star, listify, smart_open, randstr, smart_splitext
from .timestamps import timestamp_parser
from .rclonerc import IGNORED_FILE_DATA, rcpathjoin
from .threadmapper import thread_map_unordered as tmap

_r = repr

logger = logging.getLogger(__name__)


class NoTimestampInNameError(ValueError):
    pass


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

        if not (dbcache_dir := getattr(config, "dbcache_dir", None)):
            dbcache_dir = Path(dst_rclone.config_paths["Cache dir"]) / "DFB"

        self.dbcache_dir = Path(dbcache_dir)
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
                if len(r) == 2:
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
                logger.debug(f"Reference {_r(refferer)} is v1 (implied)")
                return file, referent["path"]
            elif ver == 2:
                logger.debug(f"Reference {_r(refferer)} is v2")
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
                txt = f"WARNING: File {_r(refferer)} references {_r(referent)} "
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
            logger.info(f"updated reference {_r(file['ref_rpath'])}")
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
        rc.call("sync/sync", params=params)
        logger.debug(f"sync snaps with {params = }")

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
                    f"Updated reference for {_r(file['rpath'])} "
                    f"from {_r(snapfile['rpath'])}"
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
                f"snapshot for rpath = {_r(file['rpath'])} does not match "
                "as expected. Ignoring"
            )
            return file

        # Update and reset the file
        file |= snapfile
        file["dstinfo"] = False
        return file

    def dbimport(self, exportfiles, reset=False):
        if self.config.disable_refresh:
            logger.error("DB Import not allowed due to 'disable_refresh = True")
            logger.error("Run with `--override 'disable_refresh = False'` to override")
            raise ValueError("Refresh Disabled")

        if reset:
            self.dbpath.unlink()
            self.init()

        rc = self.config.rc.start()

        # Parallelize downloading all export files. They could have all kids of different
        # paths including being full rclone paths so store by name with random component.
        # Note this goes to a tmpdir, not cache dir like refresh does since these could
        # change
        (self.config.tmpdir / "imp").mkdir(exist_ok=False, parents=True)
        exportfiles_dl = {e: f"{randstr(8)}.{os.path.basename(e)}" for e in exportfiles}

        def _dl_export(exportfile):
            rc.copyfile(
                src=exportfile,
                dst=(str(self.config.tmpdir / "imp"), exportfiles_dl[exportfile]),
                _config={"NoCheckDest": True},
            )

        for _ in tmap(_dl_export, exportfiles_dl, Nt=self.config.concurrency):
            pass

        for exportfile in exportfiles:
            logger.info(f"Importing from {_r(exportfile)}")
            files = []
            prune = []
            local_export = self.config.tmpdir / "imp" / exportfiles_dl[exportfile]
            with smart_open(str(local_export), "rt") as fp:
                for line in fp:
                    file = json.loads(line)
                    if file.get("_action", None) == "comment":
                        continue

                    if file.get("_action", None) == "prune":
                        prune.append(file)
                        continue

                    files.append(file)

            with self.db() as db:
                db.executemany(
                    f"""INSERT OR REPLACE INTO items 
                        VALUES ({','.join('?' for _ in DFBDST.COLS)})""",
                    [DFBDST.dict2fullrow(file) for file in files],
                )
            msg = f"  Imported {len(files)} files"

            if prune:
                with self.db() as db:
                    db.executemany(
                        """
                        DELETE FROM items 
                        WHERE rpath = ?""",
                        ((file["rpath"],) for file in prune),
                    )
                msg += f" and pruned {len(prune)}"

            logger.info(msg)

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
        db.commit()
        db.close()

        with open(self.config.tmpdir / f"{self.config.now.dt}Z.jsonl", "at") as fp:
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
        action = []
        if insert:
            action.append("INSERT")
        if replace:
            action.append("REPLACE")
        action = " OR ".join(action)
        sql = f"{action} INTO items VALUES ({','.join('?' for _ in DFBDST.COLS)})"

        with self.db() as db:
            db.execute(sql, DFBDST.dict2fullrow(file))

        with open(self.config.tmpdir / f"{self.config.now.dt}Z.jsonl", "at") as fp:
            print(json.dumps(file), file=fp, flush=True)

        return file

    insert = partialmethod(_insert_or_replace, insert=True, replace=False)
    replace = partialmethod(_insert_or_replace, insert=False, replace=True)
    insert_or_replace = partialmethod(_insert_or_replace, insert=True, replace=True)

    def snapshot(
        self,
        *,
        path="",
        before=None,
        after=None,
        select="*",
        export=False,
        remove_delete=True,
        delete_only=False,
        conditions=None,
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

        export [False]
            If True, includes multiples. Will override remove_delete and delete_only

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
        """
        if export:
            remove_delete = delete_only = False

        # Build the snapshot. Note that the select is never *user*
        # specified so there isn't an SQL injection risk
        query = [
            "SELECT",
            # Below we make these all a subqury when using these flags so they don't need
            # to be set here. since we will need them
            f"{select if not (remove_delete or delete_only) else '*'}",
            "FROM items",
        ]

        qvals = []
        conditions = conditions or []

        if path:
            path = path.removesuffix("/").removeprefix("./")
            conditions.append(("apath LIKE ?", f"{path}/%"))

        if before:
            b0 = before
            before = timestamp_parser(
                before,
                aware=True,
                epoch=True,
                now=self.config.now.obj,
            )
            logger.debug(f"Interpreted before = {b0} as {before} (s)")
            conditions.append(("timestamp <= ?", before))

        if after:
            a0 = after
            after = timestamp_parser(
                after,
                aware=True,
                epoch=True,
                now=self.config.now.obj,
            )
            logger.debug(f"Interpreted after = {a0} as {after} (s)")
            conditions.append(("timestamp >= ?", after))

        if conditions:
            query.append("WHERE")
            query.append(" AND ".join(cond[0] for cond in conditions))
            qvals.extend(cond[1] for cond in conditions)

        if not export:
            query.append("GROUP BY apath HAVING MAX(timestamp)")
        query.append("ORDER BY LOWER(apath)")

        query = "\n".join(query)

        outq_cond = []
        if remove_delete:
            outq_cond.append("size >= 0")
        if delete_only:
            outq_cond.append("size < 0")

        if outq_cond:  # Make all of the above a subquery. Indent for debug readability
            query = (
                f"SELECT {select} FROM (\n\n{indent(query,' '*4)}\n\n) WHERE "
                + " AND ".join(outq_cond)
            )

        db = self.db()
        with db:
            r = db.execute(query, qvals)
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

        Some of this very clever SQL came from my reddit post here:
        https://www.reddit.com/r/sqlite/comments/123bivr/comment/jdu9xvl/?context=3
        """

        # While less efficient than a single query, this works by finding the sub-items
        # and then doing additional queries on them

        conditions = conditions or []

        ###########################################################
        # All immediate files and directories (though I only need
        # directories and do files down below. I may remove that
        # from here if I can)
        # Again, See: https://www.reddit.com/r/sqlite/comments/123bivr/comment/jdu9xvl/?context=3
        query = []
        qvals = []

        subdir = subdir.removeprefix("./").removesuffix("/")
        if subdir:
            query.append(
                f"""
                WITH subpaths AS (
                    SELECT SUBSTR(apath, {len(subdir) + 2}) AS path
                    FROM items
                    WHERE apath LIKE ?
                )

                SELECT DISTINCT 
                    SUBSTR(
                        path,
                        1,
                        CASE INSTR(path, '/')
                            WHEN 0
                            THEN LENGTH(path)
                            ELSE INSTR(path, '/')
                        END
                    ) as sub
                FROM subpaths"""
            )
            qvals.append(f"{subdir}/%")
        else:
            query.append(
                """
                SELECT DISTINCT 
                    SUBSTR(
                        apath,
                        1,
                        CASE INSTR(apath, '/')
                            WHEN 0
                            THEN LENGTH(apath)
                            ELSE INSTR(apath, '/')
                        END
                    ) as sub
                FROM items"""
            )

        db = self.db()
        diritems = db.execute("\n".join(query), qvals)
        apaths = (os.path.join(subdir, row["sub"]) for row in diritems)
        ###########################################################

        if before:
            b0 = before
            before = timestamp_parser(
                before, aware=True, epoch=True, now=self.config.now.obj
            )
            logger.debug(f"Interpreted before = {b0} as {before} (s)")
            conditions.append(("timestamp <= ?", before))

        if after:
            a0 = after
            after = timestamp_parser(
                after, aware=True, epoch=True, now=self.config.now.obj
            )
            logger.debug(f"Interpreted after = {a0} as {after} (s)")
            conditions.append(("timestamp >= ?", after))

        conditions0 = conditions.copy()

        # The above does give files and directories but we really only
        # care about directories for now

        directories = []
        for apath in apaths:
            is_dir = apath.endswith("/")
            if not is_dir:
                continue
            # We need to make sure there is at least one file under
            # the directory that meets conditions (before,after,
            # optionally deleted) since they could be there
            # outside of those
            conditions = conditions0.copy()
            qvals = []

            conditions.append(["apath LIKE ?", f"{apath.removesuffix('/')}/%"])

            # We just need to find any file in the subdir. Inner query for groups
            inq = ["SELECT size FROM items"]
            inq.append("WHERE")
            inq.append(" AND ".join(cond[0] for cond in conditions))
            inq.append("GROUP BY apath HAVING MAX(timestamp)")
            inq = "\n".join(inq)

            outq = [f"SELECT * FROM ({inq})"]

            outq_cond = []
            if remove_delete:
                outq_cond.append("size >= 0")
            if delete_only:
                outq_cond.append("size < 0")
            if outq_cond:
                outq.extend(["WHERE", " AND ".join(outq_cond)])

            outq.append("LIMIT 1")  # Just one

            qvals.extend(cond[1] for cond in conditions)
            if db.execute("\n".join(outq), qvals).fetchone():
                directories.append(apath)

        ## Do files
        conditions = conditions0.copy()
        conditions.append(["apath LIKE ?", os.path.join(subdir, "%")])
        conditions.append(["apath NOT LIKE ?", os.path.join(subdir, "%", "%")])

        # Use * then let SQL downselect before return
        query = [
            """
            SELECT
                *, 
                COUNT(*) AS versions,
                SUM(
                    CASE  
                        WHEN size > 0 THEN size 
                        ELSE 0
                    END
                ) as tot_size -- Need to account for -1 vals
            FROM items
            """
        ]
        qvals = []

        query.append("WHERE")
        query.append(" AND ".join(cond[0] for cond in conditions))
        query.append("GROUP BY apath HAVING MAX(timestamp)")
        query.append("ORDER BY LOWER(apath)")

        qvals.extend(cond[1] for cond in conditions)

        qtxt = f"""
            SELECT {select} FROM (
                **sub**
            )""".replace(
            "**sub**", "\n".join(query)
        )

        outq_cond = []
        if remove_delete:
            outq_cond.append("size >= 0")
        if delete_only:
            outq_cond.append("size < 0")
        if outq_cond:
            qtxt += " WHERE " + " AND ".join(outq_cond)

        files = [DFBDST.fullrow2dict(row) for row in db.execute(qtxt, qvals)]

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


def rpath2apath(rpath):
    """
    convert rpath ('sub/dir/file.12345.txt')
    to apath ('sub/dir/file.txt').

    Can handle misplaced tags too


    Returns apath, timestamp, flag
    """
    parent, name = os.path.split(rpath)

    # Handle the special case of the tag at the end. This occurs
    # if a file doesn't have an extension or was created manually (incorrectly).
    # Otherwise, the dateflag is on the stem.
    try:
        stem, ext = os.path.splitext(name)
        ts, flag = parse_dateflag(ext[1:])
        stem, ext = os.path.splitext(stem)  # Split the rest
    except ValueError:
        # we KNOW name doesn't end in the tag and the tag isn't a valid mime-type so
        # it MUST be on the stem
        stem, ext = smart_splitext(name)
        stem, ts = os.path.splitext(stem)
        ts, flag = parse_dateflag(ts[1:])

    apath = os.path.join(parent, stem + ext)

    # Comment this out b/c older split names will give a false positive.
    # if _verify and apath2rpath(apath,ts,flag=flag,_verify=False) != rpath:
    #     logger.error(
    #         f"Failed round-trip sanity check with {apath = }, {rpath = }. "
    #         "Please submit a bug report"
    #     )
    return apath, ts, flag


def parse_dateflag(ts):
    """
    Parse the dateflag (tag) of the file. Note that while the timestamp tools
    are fairly forgiving, this one is not! It expects full date and time
    though will allow some modification around that
    """
    if ts[-1] in "DR":  # Delete, Reference
        flag = ts[-1]
        ts = ts[:-1]
    else:
        flag = ""
    ts = ts.removeprefix(".")

    allow0 = set(string.digits + "-T:.")
    allow1 = set(string.digits + ".")
    tsclean = "".join(c for c in ts if c in allow1)
    if len(tsclean) < 14 or any(c not in allow0 for c in ts):  # YYYYmmDDHHMMSS
        raise ValueError()

    ts, _, _, _ = time2all(ts)
    return ts, flag


def apath2rpath(apath, ts=None, *, flag="", verify=True):
    """
    Convert from apath,ts ('sub/dir/file.txt',12345)
    to rpath ('sub/dir/file.12345.txt')

    Will not be correct for references but *will* give the
    referrer path
    """
    ts = ts or nowfun()[0]
    ts, dt, _, _ = time2all(ts)

    base, ext = smart_splitext(apath)
    rpath = f"{base}.{dt}{flag}{ext}"

    # Comment this out b/c older split names will give a false positive.
    # if _verify and rpath2apath(rpath,_verify=False)[0] != apath:
    #     logger.error(
    #         f"Failed round-trip sanity check with {apath = }, {rpath = }. "
    #         "Please submit a bug report"
    #     )

    # Sanity check:
    if verify and rpath2apath(rpath) != (apath, ts, flag):
        logger.warning(
            f"Failed sanity check {apath = }, {rpath = }. Using fallback split"
        )
        base, ext = os.path.split(apath)
        rpath = f"{base}.{dt}{flag}{ext}"
    return rpath
