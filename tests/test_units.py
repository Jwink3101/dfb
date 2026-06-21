"""
Unit-like tests
"""

import os
import sys
import time
from types import SimpleNamespace

p = os.path.abspath("../")
if p not in sys.path:
    sys.path.insert(0, p)

from dfb.dstdb import apath2rpath, rpath2apath
from dfb.restore import Restore
from dfb.utils import (
    decode_base36_epoch,
    encode_base36_epoch,
    head_tail_table,
    parse_bytes,
    parse_timestamp_tag,
    smart_splitext,
    time2all,
    timestamp_to_epoch,
)

DATED_SPLIT_TESTS = {
    # Older style names before smart-split then test with smart
    "file.tar.20240126094501.gz": ("file.tar.20240126094501", ".gz"),
    "file.20240126094501.tar.gz": ("file.20240126094501", ".tar.gz"),
    "file.tar.20240126094501.tar.gz": ("file.tar.20240126094501", ".tar.gz"),
    "file.20240126094501": ("file", ".20240126094501"),
    # This example *shouldn't* happen in general but the following
    # is the correct behavior
    "file.txt.20240126094501": ("file", ".txt.20240126094501"),
    "file.txt.txt.20240126094501": ("file", ".txt.txt.20240126094501"),
    # No extension
    "file.20240126094501": ("file", ".20240126094501"),
}
REG_TEST = {
    # Regular
    "file.jpg": ("file", ".jpg"),
    "file": ("file", ""),
    "file.file": ("file", ".file"),
    # Hidden
    ".file.jpg": (".file", ".jpg"),
    ".file.file": (".file", ".file"),
    ".file": (".file", ""),
    # multiple. Incl. hidden and cap
    "file.tar.gz": ("file", ".tar.gz"),
    "file.TAR.gz": ("file", ".TAR.gz"),
    "file.TAR.GZ": ("file", ".TAR.GZ"),
    ".file.tar.gz": (".file", ".tar.gz"),
    # non-valid second. w/ and w/o valid earlier
    "file.tar.blaaa.gz": ("file.tar.blaaa", ".gz"),  # Stop earlier
    ".file.tar.blaaa.gz": (".file.tar.blaaa", ".gz"),
    "file.tar.blaaa.tar.gz": ("file.tar.blaaa", ".tar.gz"),
    "file.tar.tar.gz": ("file", ".tar.tar.gz"),
    # All valid but keep leading
    ".txt": (".txt", ""),
    ".tar.txt": (".tar", ".txt"),
    ".jpg.gif.tar.txt": (".jpg", ".gif.tar.txt"),
    ".txt.txt": (".txt", ".txt"),
    # Longer path
    "this/has/a.dot/file.ext": ("this/has/a.dot/file", ".ext"),
    "this/has/a.dot/file.jpg.ext": ("this/has/a.dot/file", ".jpg.ext"),
    "this/has/a.dot/noext": ("this/has/a.dot/noext", ""),
}


def test_smart_splitext():
    for inval, goldval in (REG_TEST | DATED_SPLIT_TESTS).items():
        outval = stem, ext = smart_splitext(inval)
        assert goldval == outval, f"Expected {goldval}. Got {outval}"
        assert stem + ext == inval


def test_rpath2apath():
    """
    These also  conditions that can only happen when a user modifies or writes files
    without dfb
    """
    r2a = rpath2apath
    # Normal before smart split
    assert r2a("file.tar.20240126094501.gz") == ("file.tar.gz", 1706262301, "")
    assert r2a("file.tar.20240126094501D.gz") == ("file.tar.gz", 1706262301, "D")
    assert r2a("file.tar.20240126094501R.gz") == ("file.tar.gz", 1706262301, "R")

    # Poorly formed (before smart split)
    rpath2apath("file.tar.gz.20240126094501")

    # Check all of the others
    for filename in DATED_SPLIT_TESTS:  # don't need the split
        apathG = filename.replace(".20240126094501", "")
        apath, ts, tag = r2a(filename)
        assert apath == apathG
        assert ts == 1706262301
        assert tag == ""

        apath, ts, tag = r2a(filename.replace("20240126094501", "20240126094501D"))
        assert apath == apathG
        assert ts == 1706262301
        assert tag == "D"

        apath, ts, tag = r2a(filename.replace("20240126094501", "20240126094501R"))
        assert apath == apathG
        assert ts == 1706262301
        assert tag == "R"

    # Old vs new. These are implied in the others but I want to call it out
    assert (
        r2a("file.tar.20240126094501.gz")
        == r2a("file.20240126094501.tar.gz")
        == r2a("file.tar.gz.20240126094501")
    )
    assert (
        r2a("file.TaR.20240126094501.gz")
        == r2a("file.20240126094501.TaR.gz")
        == r2a("file.TaR.gz.20240126094501")
    )

    assert (
        r2a("file.TaR.20240126094501.gZ")
        == r2a("file.20240126094501.TaR.gZ")
        == r2a("file.TaR.gZ.20240126094501")
    )

    # Test with multiple dates.
    # This is a tough edge case. If the file has a valid datetag already but does *not*
    # have an extension, this should put the tag BEFORE the existing one

    assert r2a("file.19700101000001") == ("file", 1, "")
    assert r2a("file.19700101000002.19700101000001") == ("file.19700101000001", 2, "")
    assert r2a("file.19700101000003.19700101000002.19700101000001") == (
        "file.19700101000003.19700101000001",
        2,
        "",
    )

    # There is still an edge case around something like: "file.19700101000002.jpg.19700101000001"
    # This is ambiguous! The smart-split will call 'jpg.19700101000001' the ext
    # because jpg is a MIME type but it isn't really clear. I am leaving this note but I
    # am also okay with this edge case because it comes from manual ones only!

    assert r2a("file.19700101000002.jpg.19700101000001") == (
        "file.jpg.19700101000001",
        2,
        "",
    )
    # how you get there
    assert (
        apath2rpath("file.jpg.19700101000001", "19700101000002")
        == "file.19700101000002.jpg.19700101000001"
    )

    assert r2a("file.00s7v4f1.txt") == ("file.txt", 1706262301, "")
    assert r2a("file.00S7V4F1r.txt") == ("file.txt", 1706262301, "R")
    assert r2a("file.tar.00s7v4f1.gz") == ("file.tar.gz", 1706262301, "")
    assert (
        r2a("file.tar.00s7v4f1.gz", preferred="datetag")
        == r2a("file.00s7v4f1.tar.gz", preferred="datetag")
        == r2a("file.tar.gz.00s7v4f1", preferred="datetag")
    )
    assert (
        r2a("file.tar.00s7v4f1.gz", preferred="base36-epoch")
        == r2a("file.00s7v4f1.tar.gz", preferred="base36-epoch")
        == r2a("file.tar.gz.00s7v4f1", preferred="base36-epoch")
    )

    assert r2a("file.00000002.00000001", preferred="base36-epoch") == (
        "file.00000001",
        2,
        "",
    )


def test_apath2rpath():
    a2r = apath2rpath
    for apath, split in REG_TEST.items():
        gold = split[0] + ".20240126094501" + split[1]
        assert a2r(apath, ts=1706262301, flag="") == gold
        assert a2r(apath, ts=1706262301, flag="D") == gold.replace("501", "501D")
        assert a2r(apath, ts=1706262301, flag="R") == gold.replace("501", "501R")

    apath = "03/gallery4.png@resize=376,812&ssl=1"
    rpath = apath2rpath(apath, ts=1)
    assert rpath2apath(rpath) == (apath, 1, "")

    # Multiple dates. Also test the verify
    assert "file.19700101000001.19700101000002" == apath2rpath(
        "file.19700101000002", "19700101000001"
    )
    assert "file.19700101000001.19700101000002" == apath2rpath(
        "file.19700101000002", "19700101000001", verify=True
    )
    assert "file.19700101000001.19700101000002" == apath2rpath(
        "file.19700101000002", "19700101000001", verify=False
    )

    for apath, split in REG_TEST.items():
        gold = split[0] + ".00s7v4f1" + split[1]
        assert a2r(apath, ts=1706262301, fmt="base36-epoch") == gold
        assert a2r(apath, ts=1706262301, flag="D", fmt="base36-epoch") == gold.replace(
            "4f1", "4f1D"
        )
        assert a2r(apath, ts=1706262301, flag="R", fmt="base36-epoch") == gold.replace(
            "4f1", "4f1R"
        )


def test_base36_epoch_helpers():
    assert encode_base36_epoch(0) == "00000000"
    assert encode_base36_epoch(1) == "00000001"
    assert encode_base36_epoch(1706262301) == "00s7v4f1"
    assert decode_base36_epoch("00s7v4f1") == 1706262301
    assert decode_base36_epoch("00S7V4F1") == 1706262301

    assert parse_timestamp_tag("20240126094501") == (1706262301, "", "datetag")
    assert parse_timestamp_tag("00s7v4f1") == (1706262301, "", "base36-epoch")
    assert parse_timestamp_tag("00S7V4F1d") == (1706262301, "D", "base36-epoch")
    assert parse_timestamp_tag("20240126094501", preferred="base36-epoch") == (
        1706262301,
        "",
        "datetag",
    )
    assert parse_timestamp_tag("00s7v4f1", preferred="datetag") == (
        1706262301,
        "",
        "base36-epoch",
    )

    assert timestamp_to_epoch("20240126094501") == 1706262301
    assert timestamp_to_epoch("00s7v4f1") == 1706262301


def test_time2all():
    # Make sure it will take any kind of input and give me the same
    ts = int(time.time())
    res = time2all(ts)
    for r in res:
        assert time2all(r).ts == ts


def test_restore_metadata_config():
    restore = Restore.__new__(Restore)
    restore.config = SimpleNamespace(metadata=False)
    restore._restore_metadata_state = SimpleNamespace(
        enabled=True,
        live_mode="always",
        source_slow_modtime=False,
    )

    row = {
        "rpath": "file.19700101000001.txt",
        "dstinfo": 0,
        "mtime": 123,
        "owner": "skip",
        "atime": 123,
        "x-amz-meta-color": "blue",
        "size": 12,
        "Metadata": {
            "mtime": "2024-01-02T03:04:05Z",
            "mode": "0100640",
            "btime": "2024-01-01T00:00:00Z",
            "birthtime": "2023-01-01T00:00:00Z",
            "user.comment": "kept",
        },
    }

    config = Restore._restore_metadata_config(
        restore,
        "file.19700101000001.txt",
        ("dst:", "restore/file.txt"),
        row,
    )

    assert config["Metadata"] is True
    assert config["MetadataSet"] == {
        "mtime": "1970-01-01T00:02:03Z",
        "mode": "0100640",
        "btime": "2024-01-01T00:00:00Z",
        "birthtime": "2023-01-01T00:00:00Z",
        "user.comment": "kept",
    }


def test_restore_metadata_config_uses_stored_mtime_without_metadata():
    restore = Restore.__new__(Restore)
    restore.config = SimpleNamespace(metadata=False)
    restore._restore_metadata_state = SimpleNamespace(
        enabled=True,
        live_mode="never",
        source_slow_modtime=False,
    )

    config = Restore._restore_metadata_config(
        restore,
        "file.19700101000001.txt",
        ("dst:", "restore/file.txt"),
        {
            "rpath": "file.19700101000001.txt",
            "dstinfo": 0,
            "mtime": 123.25,
        },
    )

    assert config == {
        "Metadata": True,
        "MetadataSet": {"mtime": "1970-01-01T00:02:03.250000Z"},
    }


def test_restore_metadata_config_skips_without_metadata_or_mtime():
    restore = Restore.__new__(Restore)
    restore.config = SimpleNamespace(metadata=False)
    restore._restore_metadata_state = SimpleNamespace(
        enabled=True,
        live_mode="always",
        source_slow_modtime=False,
    )

    config = Restore._restore_metadata_config(
        restore,
        "file.19700101000001.txt",
        ("dst:", "restore/file.txt"),
        {
            "rpath": "file.19700101000001.txt",
            "dstinfo": 0,
            "mode": "0100640",
        },
    )

    assert config == {"Metadata": False}


def test_restore_preload_uses_selected_snapshot_row():
    class FakeCursor:
        def __init__(self, row):
            self.row = row

        def fetchone(self):
            return self.row

    class FakeDB:
        def __init__(self, rows):
            self.rows = rows
            self.params = []

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, _sql, params):
            self.params.append(params)
            return FakeCursor(self.rows.get(params))

    class FakeDstDB:
        def __init__(self, db):
            self._db = db

        def db(self):
            return self._db

        @staticmethod
        def fullrow2dict(row):
            return row.copy()

    rows = {
        ("original.txt", 1): {
            "apath": "original.txt",
            "rpath": "shared.19700101000001.txt",
            "timestamp": 1,
            "mode": "0100600",
        },
        ("moved.txt", 3): {
            "apath": "moved.txt",
            "rpath": "shared.19700101000001.txt",
            "timestamp": 3,
            "mode": "0100640",
        },
    }
    db = FakeDB(rows)
    restore = Restore.__new__(Restore)
    restore.dstdb = FakeDstDB(db)
    restore.transfers = [
        ("shared.19700101000001.txt", ("restore", "moved.txt"), 4, "moved.txt", 3),
        (
            "shared.19700101000001.txt",
            ("restore", "original.txt"),
            4,
            "original.txt",
            1,
        ),
    ]

    Restore._preload_transfer_rows(restore)

    assert [transfer[3]["mode"] for transfer in restore.transfers] == [
        "0100640",
        "0100600",
    ]
    assert db.params == [("moved.txt", 3), ("original.txt", 1)]


def test_head_tail_table():
    table = ["head", *range(15)]

    # All items
    assert head_tail_table(table, head=0, tail=0, header=True) == table
    assert head_tail_table(table, head=0, tail=0, header=False) == table

    # First head items
    assert head_tail_table(table, head=3, tail=0, header=True) == ["head", 0, 1, 2]
    # Notice this includes the first row since it is saying there is no header
    assert head_tail_table(table, head=3, tail=0, header=False) == ["head", 0, 1]

    # Last tail items
    assert head_tail_table(table, head=0, tail=3, header=True) == ["head", 12, 13, 14]
    head_tail_table(table, head=0, tail=3, header=False) == [12, 13, 14]

    # Too long tail
    assert head_tail_table(table, head=0, tail=15, header=False) == [
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
    ]
    assert head_tail_table(table, head=0, tail=15, header=True) == table
    assert head_tail_table(table, head=0, tail=16, header=True) == table
    assert head_tail_table(table, head=0, tail=16, header=False) == table
    assert head_tail_table(table, head=0, tail=100, header=True) == table
    assert head_tail_table(table, head=0, tail=100, header=False) == table

    # too long head
    assert head_tail_table(table, head=15, tail=0, header=False) == [
        "head",
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
    ]
    assert head_tail_table(table, head=15, tail=0, header=True) == table
    assert head_tail_table(table, head=16, tail=0, header=False) == table
    assert head_tail_table(table, head=16, tail=0, header=True) == table
    assert head_tail_table(table, head=100, tail=0, header=True) == table
    assert head_tail_table(table, head=100, tail=0, header=False) == table

    # head and tail
    head_tail_table(table, head=2, tail=3, header=True) == ["head", 0, 1, 12, 13, 14]
    head_tail_table(table, head=2, tail=3, header=False) == ["head", 0, 12, 13, 14]

    # Overlap
    assert head_tail_table(table, head=8, tail=7, header=True) == table
    assert head_tail_table(table, head=9, tail=7, header=True) == table
    assert head_tail_table(table, head=8, tail=8, header=True) == table
    assert head_tail_table(table, head=9, tail=8, header=True) == table

    # Dots
    assert head_tail_table(table, head=2, tail=3, header=True, dots=True) == [
        "head",
        0,
        1,
        "...",
        12,
        13,
        14,
    ]
    assert head_tail_table(table, head=8, tail=7, header=True, dots=True) == table
    assert head_tail_table(table, head=13, tail=2, header=True, dots=True) == table
    assert head_tail_table(table, head=12, tail=2, header=True, dots=True) == [
        "head",
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        "...",
        13,
        14,
    ]
    assert head_tail_table(table, head=12, tail=1, header=True, dots=True) == [
        "head",
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        "...",
        14,
    ]
    assert head_tail_table(table, head=11, tail=2, header=True, dots=True) == [
        "head",
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        "...",
        13,
        14,
    ]
    assert head_tail_table(table, head=11, tail=4, header=True, dots=True) == table
    assert head_tail_table(table, head=11, tail=4, header=False, dots=True) == [
        "head",
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        "...",
        11,
        12,
        13,
        14,
    ]
    assert head_tail_table(table, head=1, tail=13, dots=True) == [
        "head",
        0,
        "...",
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
    ]
    assert head_tail_table(table, head=1, tail=13, header=False, dots=True) == [
        "head",
        "...",
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
    ]

    # Test with full table. Mostly affects dots
    assert head_tail_table([[1, 2], [3, 4], [5, 6], [7, 8]], header=False, head=1) == [
        [1, 2]
    ]
    assert head_tail_table([[1, 2], [3, 4], [5, 6], [7, 8]], header=False, tail=1) == [
        [7, 8]
    ]
    assert head_tail_table(
        [[1, 2], [3, 4], [5, 6], [7, 8]], header=False, head=1, tail=1
    ) == [[1, 2], [7, 8]]
    assert head_tail_table(
        [[1, 2], [3, 4], [5, 6], [7, 8]], header=False, head=1, tail=1, dots=True
    ) == [[1, 2], ["...", "..."], [7, 8]]


def test_parse_bytes():
    tests = [
        ("23234", 23234),
        ("23234b", 23234),
        ("123k", 123 * 1000),
        ("123 kilobytes", 123 * 1000),
        ("123 kb", 123 * 1000),
        ("123 kib", 123 * 1024),
        ("123 kibi", 123 * 1024),
        ("123 kibibytes", 123 * 1024),
        ("123mi", 128974848),
        ("12.3tB", 12300000000000),
        ("12.3t", 12300000000000),
        ("12.3ti", 13523993021644),
        ("12.3tib", 13523993021644),
        ("0.0002t", 200000000),
        (12345, 12345),
        (12.345, 12),
    ]

    for inval, gold in tests:
        assert parse_bytes(inval) == gold


if __name__ == "__main__":
    # Names and split
    test_smart_splitext()
    test_rpath2apath()
    test_apath2rpath()

    # Others
    test_time2all()
    test_head_tail_table()
    test_parse_bytes()

    print("=" * 50)
    print(" All Passed ".center(50, "="))
    print("=" * 50)
