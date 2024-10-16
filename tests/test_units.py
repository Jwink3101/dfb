"""
Unit-like tests
"""

import os, sys, time

p = os.path.abspath("../")
if p not in sys.path:
    sys.path.insert(0, p)

from dfb.utils import smart_splitext, time2all, head_tail_table, parse_bytes
from dfb.dstdb import rpath2apath, apath2rpath

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


def test_time2all():
    # Make sure it will take any kind of input and give me the same
    ts = int(time.time())
    res = time2all(ts)
    for r in res:
        assert time2all(r).ts == ts


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
