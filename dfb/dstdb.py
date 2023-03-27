"""
Database of the destination. Includes the tools and methods to refresh it.
"""
import os
from pathlib import Path
import sqlite3
import time
import json
from functools import partialmethod
from textwrap import dedent

from . import __version__, log, debug, nowfun
from .utils import time2all, MyRow, star
from .timestamps import timestamp_parser
from .rclone import IGNORED_FILE_DATA
from .threadmapper import thread_map_unordered as tmap


class NoTimestampInNameError(ValueError):
    pass


def sqldebug(sql):
    sql = "\n".join(line for line in sql.split("\n") if line.strip())
    sql = dedent(sql).rstrip()
    log(f">>>>>>>>>>>>>>> DSTDB\n{sql}\n<<<<<<<<<<<<<<<", prefix="sql", verbosity=3)


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

        dbpath = (
            Path(dst_rclone.config_paths["Cache dir"]) / "DFB" / f"{config._uuid}.db"
        )
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
                    debug(f"dstdb exists. {created = } {version = }")
                    return
        except:
            debug("Recreate dstdb")

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

    def reset(self, stats=None):
        self.dbpath.unlink()
        self.init()

        # ALWAYS wait before an executemany since that could lock the DB
        files = list(self._relist(stats=stats))

        with self.db() as db:
            db.executemany(
                f"""
                INSERT INTO items VALUES ({','.join('?' for _ in DFBDST.COLS)})""",
                files,
            )
        db.commit()
        db.close()

        # Update those with isref = 2
        self._update_references()

    def _relist(self, stats=None):
        config = self.config
        flags = config.dst_list_rclone_flags

        files = config.dst_rclone.listremote(
            mimetype=False,
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
            except NoTimestampInNameError:
                debug(f"Could not find timestamp for {file['Path']}. Ignoring")
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
                log(f"Destination Listing Status: {c} items")
                t0 = time.time()

            yield DFBDST.dict2fullrow(new)

    def _update_references(self):
        db = self.db()
        with db:
            files = db.execute("""SELECT * FROM items WHERE isref = 2""")
            files = files.fetchall()

        # Multi-thread reading from the remote to get the new rpath
        # and reading from the DB to get the info
        rc = self.config.rc
        rc.start_rc()

        def _get_referent(file):
            refferer = file["rpath"]
            referent = rc.read((self.config.dst, refferer)).decode()
            return file, referent

        files = tmap(_get_referent, files, Nt=self.config.concurrency)

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
                txt = f"WARNING: File {repr(refferer)} references {repr(referent)} "
                txt += "but it is missing. Will just be treated as deleted"
                log(txt, verbosity=0)
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
        # ALWAYS wait before an executemany since that could lock the DB
        files = map(DFBDST.dict2fullrow, files)
        files = list(files)
        with db:
            db.executemany(
                f"REPLACE INTO items VALUES ({','.join('?' for _ in DFBDST.COLS)})",
                files,
            )
        db.commit()
        db.close()

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
        return files

    insert_many = partialmethod(insert_or_replace_many, insert=True, replace=False)
    replace_many = partialmethod(insert_or_replace_many, insert=False, replace=True)

    def insert_or_replace(self, file, *, insert, replace):
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

        with self.db() as db:
            db.execute(sql, DFBDST.dict2fullrow(file))
        db.commit()
        return file

    insert = partialmethod(insert_or_replace, insert=True, replace=False)
    replace = partialmethod(insert_or_replace, insert=False, replace=True)

    def snapshot(
        self,
        *,
        path="",
        before=None,
        after=None,
        select="*",
        remove_delete=True,
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
            than manual filtering

        conditions:
            List of additional (sql,val) pairs. Warning: Do not let sql be user input.
            Examples: ('apparentparent LIKE ?','a/sub/path/')

            WARNING: Do not do ('size >= ?',0) since that will then include the non-deleted
                     version. It is better to filter it later.
        """
        # Build the snapshot. Note that the select is never *user*
        # specified so there isn't an SQL injection risk
        query = [f"SELECT {select if not remove_delete else '*'} FROM items"]

        qvals = []
        conditions = conditions or []

        if path:
            path = path.rstrip("/")
            if path.startswith("./"):
                path = path[2:]
            conditions.append(("apath LIKE ?", f"{path}/%"))

        if before:
            b0 = before
            before = timestamp_parser(
                before, aware=True, epoch=True, now=self.config.now.obj
            )
            debug(f"Interpreted before = {b0} as {before} (s)")
            conditions.append(("timestamp <= ?", before))

        if after:
            a0 = after
            after = timestamp_parser(
                after, aware=True, epoch=True, now=self.config.now.obj
            )
            debug(f"Interpreted after = {a0} as {after} (s)")
            conditions.append(("timestamp >= ?", after))

        if conditions:
            query.append("WHERE")
            query.append(" AND ".join(cond[0] for cond in conditions))
            qvals.extend(cond[1] for cond in conditions)

        query.append("GROUP BY apath HAVING MAX(timestamp)")
        query.append("ORDER BY LOWER(apath)")
        query = "\n".join(query)

        if remove_delete:  # select is * above so use it here
            query = f"SELECT {select} FROM ({query}) WHERE size >= 0"

        db = self.db()
        with db:
            r = db.execute(query, qvals)
        return r

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

    def group_by_apath(self, select="*"):
        """
        Group by apath where each group will be sorted by timestamp.
        (so you can use bisect to quickly find elements)

        Can change select but MUST include apath
        """
        db = self.db()
        with db:
            Qres = db.execute(
                f"""
                SELECT {select} FROM items
                ORDER BY
                    LOWER(apath),timestamp"""
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
        rowdict = rowdict.copy()

        cs = rowdict.get("checksum", None)
        if cs:
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

    Does not work for reference links
    """
    parent, name = os.path.split(rpath)
    dot = ""
    if name.startswith("."):
        dot = "."
        name = name[1:]
    name = name.split(".")
    if len(name) == 1:
        raise NoTimestampInNameError(rpath)
    if len(name) == 2:  # no extension
        aname, ts = name
    else:
        *aname0, ts, ext = name
        aname = ".".join(aname0) + f".{ext}"
    aname = dot + aname
    apath = os.path.join(parent, aname)

    if ts[-1] in "DR":  # Delete, Reference
        flag = ts[-1]
        ts = ts[:-1]
    else:
        flag = ""

    # Undocumented but it can handle any timestamp in the name
    ts, _, _, _ = time2all(ts)

    return apath, ts, flag


def apath2rpath(apath, ts=None, *, flag=""):
    """
    Convert from apath,ts ('sub/dir/file.txt',12345)
    to rpath ('sub/dir/file.12345.txt')

    Will not be correct for references but *will* give the
    referrer path
    """
    ts = ts or nowfun()[0]
    _, dt, _, _ = time2all(ts)

    base, ext = os.path.splitext(apath)
    return f"{base}.{dt}{flag}{ext}"
