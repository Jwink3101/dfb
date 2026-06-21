import datetime
import sqlite3
from types import SimpleNamespace

import dfb.dstdb
from dfb.dstdb import DFBDST


def make_dstdb_config(tmp_path):
    now = SimpleNamespace(
        obj=datetime.datetime(2026, 6, 20),
        dt="20260620134300",
        ts=1781984580,
    )
    return SimpleNamespace(
        _config={"dst_rclone": None},
        dbcache_dir=tmp_path / "db",
        snap_cache_dir=tmp_path / "snap",
        now=now,
        config_id="test",
    )


def make_file(index):
    return {
        "rpath": f"file.{index}.txt",
        "apath": f"file{index}.txt",
        "timestamp": index,
        "size": index,
        "mtime": None,
        "checksum": None,
        "isref": 0,
        "ref_rpath": None,
        "dstinfo": 0,
    }


def test_dstdb_init_existing_db_does_not_reuse_closed_connection(tmp_path):
    config = make_dstdb_config(tmp_path)

    DFBDST(config)
    DFBDST(config)


def test_insert_and_snapshot_reuse_dstdb_connection(tmp_path, monkeypatch):
    connect_count = 0
    orig_connect = sqlite3.connect

    def connect(*args, **kwargs):
        nonlocal connect_count
        connect_count += 1
        return orig_connect(*args, **kwargs)

    monkeypatch.setattr(dfb.dstdb.sqlite3, "connect", connect)

    dstdb = DFBDST(make_dstdb_config(tmp_path))
    assert connect_count == 1

    dstdb.insert_many([make_file(i) for i in range(3)])
    assert connect_count == 1

    rows = dstdb.snapshot(export=True)
    assert len(list(rows)) == 3
    assert connect_count == 1

    count = dstdb.snapshot(select="COUNT(*) AS n", export=True)
    assert count.fetchone()["n"] == 3
    assert connect_count == 1
