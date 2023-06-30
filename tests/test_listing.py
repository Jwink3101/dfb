#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
from pathlib import Path
import gzip as gz
import subprocess
import json
import shutil
from textwrap import dedent

p = os.path.abspath("../")
if p not in sys.path:
    sys.path.insert(0, p)

# Local
import testutils

# testing
import pytest


def test_listing():
    test = testutils.Tester(name="listing")

    test.config["renames"] = "mtime"
    test.write_config()

    test.write_pre("src/untouched.txt", "Never Modified")
    test.write_pre("src/sub1/file.txt", "Always move")
    test.write_pre("src/new1.txt", "new 1")
    test.write_pre("src/mod.txt", ".")
    test.write_pre("src/n1d3.txt", "new at 1, delete at 3")
    test.backup("-q", offset=1)

    test.move("src/sub1/file.txt", "src/sub2/file.txt")
    test.write_pre("src/new2.txt", "new 2")
    test.write_pre("src/mod.txt", "..")
    test.backup("-q", offset=2)

    test.move("src/sub2/file.txt", "src/sub3/file.txt")
    test.write_pre("src/new3.txt", "new 3")
    test.write_pre("src/mod.txt", "...")
    os.unlink("src/n1d3.txt")
    test.backup("-q", offset=3)

    test.move("src/sub3/file.txt", "src/sub4/file.txt")
    test.write_pre("src/new4.txt", "new 4")
    test.write_pre("src/mod.txt", "....")
    test.backup("-q", offset=4)

    # # # ls

    test.call("ls", "--no-header")
    items = {i.strip() for i in test.logs[-1][0].split("\n") if i.strip()}
    assert items == {
        "mod.txt",
        "new1.txt",
        "new2.txt",
        "new3.txt",
        "new4.txt",
        "sub4/",
        "untouched.txt",
    }

    test.call("ls", "--at", "u3.1", "--no-header")
    items = {i.strip() for i in test.logs[-1][0].split("\n") if i.strip()}
    assert items == {
        "mod.txt",
        "new1.txt",
        "new2.txt",
        "new3.txt",
        "sub3/",
        "untouched.txt",
    }

    test.call("ls", "--at", "u2.1", "--no-header")
    items = {i.strip() for i in test.logs[-1][0].split("\n") if i.strip()}
    assert items == {
        "mod.txt",
        "new1.txt",
        "n1d3.txt",
        "new2.txt",
        "sub2/",
        "untouched.txt",
    }

    test.call("ls", "--at", "u1.1", "--no-header")
    items = {i.strip() for i in test.logs[-1][0].split("\n") if i.strip()}
    assert items == {"mod.txt", "new1.txt", "n1d3.txt", "sub1/", "untouched.txt"}

    test.call("ls", "-d", "--no-header")
    items = {i.strip() for i in test.logs[-1][0].split("\n") if i.strip()}
    assert items == {
        "mod.txt",
        "n1d3.txt (DEL)",
        "new1.txt",
        "new2.txt",
        "new3.txt",
        "new4.txt",
        "sub1/",
        "sub2/",
        "sub3/",
        "sub4/",
        "untouched.txt",
    }

    test.call("ls", "-d", "--at", "u2.1")
    test.call("ls", "-d", "--at", "u2.1", "--no-header")
    items = {i.strip() for i in test.logs[-1][0].split("\n") if i.strip()}
    assert items == {
        "mod.txt",
        "n1d3.txt",
        "new1.txt",
        "new2.txt",
        "sub1/",
        "sub2/",
        "untouched.txt",
    }

    # Before and afters and only. Also test using a delta
    test.call("ls", "--no-header", "--before", "u3", "--after", "u3")
    items = {i.strip() for i in test.logs[-1][0].split("\n") if i.strip()}
    assert items == {"mod.txt", "new3.txt", "sub3/"}

    test.call("ls", "--no-header", "--only", "u3")
    items = {i.strip() for i in test.logs[-1][0].split("\n") if i.strip()}
    assert items == {"mod.txt", "new3.txt", "sub3/"}

    # Test using a delta. Use offset so this is u3
    test.call("ls", "--only", "3.5 seconds", "--no-header", offset=6.5)
    items = {i.strip() for i in test.logs[-1][0].split("\n") if i.strip()}
    assert items == {"mod.txt", "new3.txt", "sub3/"}

    test.call("ls", "sub1", "-d", "--no-header")
    items = {i.strip() for i in test.logs[-1][0].split("\n") if i.strip()}
    assert items == {"file.txt (DEL)"}

    test.call("ls", "sub1", "-d", "--full-path", "--no-header")
    items = {i.strip() for i in test.logs[-1][0].split("\n") if i.strip()}
    assert items == {"sub1/file.txt (DEL)"}

    # I am going to stop checking output since the modtimes will mess it up but this
    # will test the code path and I have verified it manually
    test.call("ls", "-l")
    test.call("ls", "-ll")
    test.call("ls", "-lld")
    test.call("ls", "-lld", "--timestamp-local", "--human")

    test.call("ls", "sub1dasdas")

    test.call("timestamps", "--human")

    # ## Snapshots
    test.call("timestamps")
    test.call("timestamps", "--human", "--timestamp-local")

    # +
    test.call("snapshot")
    clisnap = [json.loads(line) for line in test.logs[-1][0].splitlines() if line]
    clisnap = {f["apath"] for f in clisnap}

    assert clisnap == {dict(f)["apath"] for f in test.remote_snapshot()}
    # -

    for ts in [1, 2, 3, 4]:
        test.call("snapshot", "--at", f"u{ts+0.1}", "--output", "tmp.jsonl")
        clisnap = [json.loads(line) for line in open("tmp.jsonl") if line]
        clisnap = {f["apath"] for f in clisnap}
        assert clisnap == {
            dict(f)["apath"] for f in test.remote_snapshot(before=ts + 0.1)
        }

    # Like ls -l, this is hard to verify with ModTime. Just testing code paths.
    # It has been verified that it works
    test.call("versions", "mod.txt")
    test.call("versions", "mod.txt", "--timestamp-local")
    test.call("versions", "mod.txt", "--ref-count")

    test.call("versions", "sub1/file.txt", "--ref-count", "--real-path")
    test.call("versions", "sub2/file.txt", "--ref-count", "--real-path")
    test.call("versions", "sub3/file.txt", "--ref-count", "--real-path")
    test.call("versions", "sub4/file.txt", "--ref-count", "--real-path")
    test.call("versions", "sub4/file.txt", "--ref-count", "--real-path", "--real-path")

    test.call("versions", "made up file", "--ref-count", "--real-path")


def test_del():
    test = testutils.Tester(name="listing_del")

    test.config["renames"] = "mtime"
    test.write_config()

    test.write_pre("src/untouched.txt", "Never Modified")
    test.write_pre("src/del_at_3.txt", "delete at 3")
    test.write_pre("src/sub_del_at_3/file.txt", "delete at 3 --sub")
    test.write_pre("src/sub_del_at_5/file.txt", "delete at 5 --sub")
    test.write_pre("src/mv1/f1.txt", "move each time")
    test.write_pre("src/new1/new1.txt", "new 1")

    test.backup(offset=1)

    os.unlink("src/del_at_3.txt")
    shutil.rmtree("src/sub_del_at_3")
    test.move("src/mv1/f1.txt", "src/mv2/f2.txt")
    os.rmdir("src/mv1")
    test.write_pre("src/new3/new3.txt", "new 2")

    test.backup(offset=3)

    shutil.rmtree("src/sub_del_at_5")
    test.move("src/mv2/f2.txt", "src/mv3/f3.txt")
    os.rmdir("src/mv2")
    test.write_pre("src/new5/new5.txt", "new 2")

    test.backup(offset=5)

    test.call("ls", "--no-header")
    items = {i.strip() for i in test.logs[-1][0].split("\n") if i.strip()}
    assert items == {"mv3/", "new1/", "new3/", "new5/", "untouched.txt"}

    test.call("ls", "--no-header", "--del")
    items = {i.strip() for i in test.logs[-1][0].split("\n") if i.strip()}
    assert items == {
        "del_at_3.txt (DEL)",
        "mv1/",
        "mv2/",
        "mv3/",
        "new1/",
        "new3/",
        "new5/",
        "sub_del_at_3/",
        "sub_del_at_5/",
        "untouched.txt",
    }

    test.call("ls", "--no-header", "--del", "--del")
    items = {i.strip() for i in test.logs[-1][0].split("\n") if i.strip()}
    assert items == {
        "del_at_3.txt (DEL)",
        "mv1/",
        "mv2/",
        "sub_del_at_3/",
        "sub_del_at_5/",
    }

    test.call("snapshot", "--output", "0.jsonl")
    test.call("snapshot", "--del", "--output", "1.jsonl")
    test.call("snapshot", "--del", "--del", "--output", "2.jsonl")

    with open("0.jsonl") as fp:
        i0 = {(item["apath"], item["size"]) for item in map(json.loads, fp)}
    assert i0 == {
        ("mv3/f3.txt", 14),
        ("new1/new1.txt", 5),
        ("new3/new3.txt", 5),
        ("new5/new5.txt", 5),
        ("untouched.txt", 14),
    }

    with open("1.jsonl") as fp:
        i1 = {(item["apath"], item["size"]) for item in map(json.loads, fp)}
    assert i1 == {
        ("del_at_3.txt", -1),
        ("mv1/f1.txt", -1),
        ("mv2/f2.txt", -1),
        ("mv3/f3.txt", 14),
        ("new1/new1.txt", 5),
        ("new3/new3.txt", 5),
        ("new5/new5.txt", 5),
        ("sub_del_at_3/file.txt", -1),
        ("sub_del_at_5/file.txt", -1),
        ("untouched.txt", 14),
    }

    with open("2.jsonl") as fp:
        i2 = {(item["apath"], item["size"]) for item in map(json.loads, fp)}
    assert i2 == {
        ("del_at_3.txt", -1),
        ("mv1/f1.txt", -1),
        ("mv2/f2.txt", -1),
        ("sub_del_at_3/file.txt", -1),
        ("sub_del_at_5/file.txt", -1),
    }

    assert i0.union(i2) == i1


if __name__ == "__main__":
    # test_listing()
    test_del()
    print("=" * 50)
    print(" All Passed ".center(50, "="))
    print("=" * 50)
