"""
A local DB of checksums
"""
from pathlib import Path
import sqlite3
import json

from . import __version__, log, debug
from .utils import MyRow

_r = repr


def sqldebug(sql):
    sql = "\n".join(line for line in sql.split("\n") if line.strip())
    sql = dedent(sql).rstrip()
    log(f"+++ ChecksumDB\n{sql}\n---", prefix="sql", verbosity=3)


class SourceChecksumDB:
    def __init__(self, config):
        self.config = config
        self.src_rclone = src_rclone = config._config["src_rclone"]

        dbpath = (
            Path(src_rclone.config_paths["Cache dir"])
            / "DFB"
            / f"{config._uuid}.checksum.db"
        )
        debug(f"checksum db {dbpath = }")
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
                    debug(f"hashdb exists. {created = } {version = }")
                    return
        except:
            debug("Recreate hash DB")

        with db:
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS src(
                    apath TEXT PRIMARY KEY,
                    size INTEGER,
                    mtime REAL,
                    checksum TEXT
                )
                """
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

    def add_checksum_to_file(self, file):
        """Add checksum to the file dict"""
        db = self.db()

        q = [file["apath"], file["size"], file.get("mtime", 0)]
        if self.config.reuse_hashes == "mtime":
            q.append(self.config.dt)
        else:
            q.append(float("inf"))  # Any time tolerance

        with db:
            row = db.execute(
                """
                SELECT checksum
                FROM src
                WHERE
                    apath = ? AND size = ? AND ABS(mtime - ?) < ?""",
                q,
            )
        row = row.fetchone()

        if row and (cs := json.loads(row["checksum"])):
            file["checksum"] = cs
        return file

    def update_db(self, files):
        """Update the DB with the files. This should NOT be multi-threaded"""
        files = [file for file in files if file.get("checksum", None)]
        if not files:
            debug("no files to update")
            return

        rows = (
            [f["apath"], f["size"], f["mtime"], json.dumps(f["checksum"])]
            for f in files
        )

        # ALWAYS wait before an executemany since that could lock the DB. Note that this
        # is called with all items already done (from an rclone call) so there is no
        # need to
        rows = list(rows)

        db = self.db()
        with db:
            db.executemany(
                """
                INSERT OR REPLACE INTO src
                VALUES (?,?,?,?)
                """,
                rows,
            )
        db.commit()
        db.close()
