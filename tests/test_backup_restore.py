#!/usr/bin/env python
# -*- coding: utf-8 -*-

import gzip as gz
import itertools
import json
import lzma as xz
import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from textwrap import dedent

p = os.path.abspath("../")
if p not in sys.path:
    sys.path.insert(0, p)

# testing
import pytest

# Local
import testutils

from dfb.backup import NoCommonHashError
from dfb.cli import cli
from dfb.utils import smart_splitext


@pytest.mark.parametrize("rename_method", ["reference", "copy"])
def test_main(rename_method):
    test = testutils.Tester(name="main")

    test.config["renames"] = "mtime"
    test.config["filter_flags"] = ["--filter", "- *.exc"]
    test.config["rename_method"] = rename_method

    # # Main Test -- Tracking

    test.write_config()

    # +
    test.write_pre("src/versions.txt", "versions 1.")
    test.write_pre(
        "src/unįçôde, spaces, symb°ls (!@#$%^&*) in €‹›ﬁﬂ‡°·—±",
        "did it work?",
    )
    test.write_pre("src/untouched.txt", "Do not modify")
    test.write_pre("src/mod_same-size.txt", "modify but not change size")
    test.write_pre("src/mod_diff_size.txt", "modify and change size")
    test.write_pre(
        "src/mod_same-size-mtime.txt",
        "modify but not change size OR modTime",
    )
    test.write_pre("src/touch.txt", "touch me", dt=-1000)
    test.write_pre("src/sub/move.txt", "move me")
    test.write_pre("src/skip.exc", "skip me")
    test.write_pre("src/delete.txt", "delete me")

    test.write_pre("src/two_mv-1.txt", "Two will move")
    shutil.copy2("src/two_mv-1.txt", "src/two_mv-2.txt")

    test.backup("-v", offset=1)
    # -

    diff = test.src_missing_in_dst(keys=("apath", "size"))
    assert diff == {frozenset({("apath", "skip.exc"), ("size", 7)})}  # skip

    # +
    # Modify
    test.write_pre("src/versions.txt", "versions 3..")
    test.write_pre("src/new_at_3.txt", "I was created at 3")

    test.write_post("src/mod_same-size.txt", "ModifY BuT NoT ChangE SizE")
    test.write_post("src/mod_diff_size.txt", "modify and change size --DONE")

    stat0 = os.stat("src/mod_same-size-mtime.txt")
    test.write_post(
        "src/mod_same-size-mtime.txt", "MODIFY but Not change size OR modTime"
    )
    os.utime("src/mod_same-size-mtime.txt", (stat0.st_atime, stat0.st_mtime))

    Path("src/touch.txt").touch()
    test.move("src/sub/move.txt", "src/sub2/moved.txt")
    os.unlink("src/delete.txt")

    test.move("src/two_mv-1.txt", "src/two_moved-1.txt")
    test.move("src/two_mv-2.txt", "src/two_moved-2.txt")

    back = test.backup(offset=3)
    log = test.logs[-1][0]

    # -

    diff = test.src_missing_in_dst(keys="apath")

    assert diff == {
        frozenset({("apath", "mod_same-size-mtime.txt")}),
        frozenset({("apath", "skip.exc")}),
    }

    assert not any(
        ("apath", "new_at_3.txt") in f for f in test.remote_snapshot(before=2)
    )
    assert any(("apath", "new_at_3.txt") in f for f in test.remote_snapshot(before=4))

    # Test the move
    refitem = test.dstdb.snapshot(
        conditions=[("apath = :pp", {"pp": "sub2/moved.txt"})]
    ).fetchone()
    refitem = test.dstdb.fullrow2dict(refitem)

    if rename_method == "reference":
        r = json.loads(test.dst_rclone.read("sub2/moved.19700101000003R.txt"))
        assert r == {"ver": 2, "rel": "../sub/move.19700101000001.txt"}

        # DB
        assert refitem.get("isref", False)
        assert refitem["rpath"] == "sub/move.19700101000001.txt"
    else:
        assert test.read("dst/sub2/moved.19700101000003.txt") == "move me"

        # DB
        assert not refitem.get("isref", False)
        assert refitem.get("source_rpath", None) == "sub/move.19700101000001.txt"
        assert refitem.get("original", None) == "sub/move.txt"

    test.write_pre("src/versions.txt", "versions 5...")
    test.move("src/sub2/moved.txt", "src/moved_again.txt")
    test.backup("--refresh", offset=5)  # add refresh to test that too

    # Should still point to the original!
    if rename_method == "reference":
        r = json.loads(test.dst_rclone.read("moved_again.19700101000005R.txt"))
        assert r == {"ver": 2, "rel": "sub/move.19700101000001.txt"}
    else:
        test.read("dst/moved_again.19700101000005.txt") == "move me"

    diff = test.src_missing_in_dst(keys="apath")
    assert diff == {
        frozenset({("apath", "mod_same-size-mtime.txt")}),
        frozenset({("apath", "skip.exc")}),
    }

    assert "Too many matches for 'two_moved-1.txt'. Not moving" in log
    assert "Too many matches for 'two_moved-2.txt'. Not moving" in log

    # # Secondary Tests

    # ## Does a refresh look like the original

    # + tags=[]
    test.call("snapshot", "--output", "A.jsonl")

    # Use this opportunity to also test disable_refresh. -vv makes it throw error
    try:
        test.call("refresh", "-o", "disable_refresh = True", "-vv")
        assert False
    except ValueError:
        pass

    test.call("refresh")
    test.call("snapshot", "--output", "B.jsonl")

    with open("A.jsonl") as fp:
        A = [json.loads(line) for line in fp]
    with open("B.jsonl") as fp:
        B = [json.loads(line) for line in fp]

    # no mtime since precision issues
    keep = {"rpath", "apath", "timestamp", "size", "ref_rpath"}
    A = [{k: v for k, v in item.items() if k in keep} for item in A]
    B = [{k: v for k, v in item.items() if k in keep} for item in B]
    A = {frozenset(item.items()) for item in A}
    B = {frozenset(item.items()) for item in B}
    assert A == B

    ## Test reference format v2
    test.call("refresh", "-vv", "--no-use-snapshot")
    log = test.logs[-1][0]
    if rename_method == "reference":
        assert "Reference 'moved_again.19700101000005R.txt' is v2" in log

        # test format v1
        with open("dst/moved_again.19700101000005R.txt", "wt") as fp:
            fp.write("sub/move.19700101000001.txt")
        test.call("refresh", "-vv", "--no-use-snapshot")
        log = test.logs[-1][0]
        assert "Reference 'moved_again.19700101000005R.txt' is v1 (implied)" in log

        # Do it again with snapshot
        test.call("refresh", "-vv")
        log = test.logs[-1][0]
        assert (
            "Updated reference for 'moved_again.19700101000005R.txt' "
            "from 'sub/move.19700101000001.txt"
        ) in log

    # ## Do the snapshots match the reality?

    # Now that we are done, check all of the snapshots as read using the different tools
    # This tests the historic snapshots agains what they were.
    for ts, loc in test.backup_local_files.items():
        rem = test.remote_snapshot(before=ts + 0.1)
        miss_rem = {
            dict(a)["apath"] for a in loc - rem
        }  # Just the paths, but the whole thing has to agree
        miss_loc = {dict(a)["apath"] for a in rem - loc}  # ...

        assert miss_rem - {"skip.exc", "mod_same-size-mtime.txt"} == set()
        assert miss_loc - {"mod_same-size-mtime.txt"} == set()

    # ## Does restore match reality?
    #
    # Do a bona-fide restore and see

    for ts, loc in test.backup_local_files.items():
        test.call("restore", "--at", f"u{ts+0.1}", str(test.pwd / f"ts{ts}"))
        rem = test.local_files(test.pwd / f"ts{ts}")

        miss_rem = {
            dict(a)["apath"] for a in loc - rem
        }  # Just the paths, but the whole thing has to agree
        miss_loc = {dict(a)["apath"] for a in rem - loc}  # ...

        assert miss_rem - {"skip.exc", "mod_same-size-mtime.txt"} == set()
        assert miss_loc - {"mod_same-size-mtime.txt"} == set()

    # ## Do restore scripts match?
    #
    # Same as above but using the restore script

    for ts, loc in test.backup_local_files.items():
        restore_dir = str(test.pwd / f"script_ts{ts}")
        restore_script = str(test.pwd / f"script_ts{ts}.sh")
        test.call(
            "restore",
            "--at",
            f"u{ts+0.1}",
            restore_dir,
            "--shell-script",
            restore_script,
        )
        subprocess.check_call(["bash", restore_script])

        rem = test.local_files(restore_dir)

        miss_rem = {
            dict(a)["apath"] for a in loc - rem
        }  # Just the paths, but the whole thing has to agree
        miss_loc = {dict(a)["apath"] for a in rem - loc}  # ...

        assert miss_rem - {"skip.exc", "mod_same-size-mtime.txt"} == set()
        assert miss_loc - {"mod_same-size-mtime.txt"} == set()

    # ## Misc
    #
    # just to test some code paths
    test.call("restore-file", "sub/move.txt", ".", "--dry-run", "--at", "u1")
    test.call("restore-file", "sub/move.txt", ".", "--at", "1970-01-01 00:00:01Z")
    test.call(
        "restore-file",
        "sub/move.txt",
        "-",
        "--at",
        "1970-01-01 00:00:01Z",
        "--shell-script",
        "-",
    )

    out = test.ls("-d", "--after", "u4", "--no-header")
    items = {l.strip() for l in out.split("\n") if l.strip()}
    assert items == {"versions.txt", "moved_again.txt", "sub2/"}

    cli(["init", "new.py"])
    cli(["init", "new.py"])
    cli(["init", "new.py", "--force-overwrite"])

    test.config["rclone_env"]["RCLONE_CONFIG_PASS"] = "secret"
    test.write_config()
    r = repr(test.config_obj)
    assert "secret" not in r
    assert "**REDACTED**" in r


def test_shell():
    test = testutils.Tester(name="shell")

    # Configure each of the shell types. Need two tries...

    # +
    test.config["pre_shell"] = dedent(
        """\
            echo PRESHELL
            echo PWD = $PWD
            echo CONFIGDIR = $CONFIGDIR"""
    )
    test.config["post_shell"] = [
        "python",
        "-c",
        dedent(
            """\
        print("Post Shell")
        import os,sys
        print(f"{os.getcwd() = }")
        sys.exit(10)"""
        ),
    ]

    test.config["stop_on_shell_error"] = False
    # -

    test.write_config()

    test.write_pre("src/file.txt", "file")
    test.backup(offset=1)

    # +
    out = test.logs[-1][0]
    assert "pre.shell: $ echo PRESHELL" in out
    assert "pre.shell: $ echo PWD = $PWD" in out
    assert "pre.shell: $ echo CONFIGDIR = $CONFIGDIR" in out
    assert "out: PRESHELL" in out
    assert "PWD = /" in out
    assert "CONFIGDIR = /" in out

    assert (
        r"""post.shell ['python', '-c', 'print("Post Shell")\nimport os,sys\nprint(f"{os.getcwd() = }")\nsys.exit(10)']"""
        in out
    )

    assert "out: Post Shell" in out
    assert "os.getcwd() = '/" in out
    # -

    test.config["post_shell"] = dict(
        cmd=[
            "python",
            "-c",
            dedent(
                """\
                import os
                print(f"{os.getcwd() = }")
                print(f"{os.environ.get('SHELL_TEST','FAIL') = }")
                print(f"{os.environ.get('CONFIGDIR','FAIL') = }")
                print(f"{os.environ.get('STATS','FAIL') = }")
                """
            ),
        ],
        shell=False,
        cwd=os.path.expanduser("~/"),
        env={"SHELL_TEST": "SUCCESS"},
    )
    test.config["pre_shell"] = ""
    test.write_config()

    test.write_pre("src/file.txt", "file3")
    test.backup(offset=3)

    out = test.logs[-1][0]
    assert "os.getcwd() = '/" in out
    assert "os.environ.get('SHELL_TEST','FAIL') = 'SUCCESS'" in out
    assert "os.environ.get('CONFIGDIR','FAIL') = '/" in out


def test_log_upload():
    test = testutils.Tester(name="log_upload")

    test.write_config()

    test.write_pre("src/versions.txt", "versions 1.")
    test.backup(offset=1)
    assert os.path.exists("dst/.dfb/logs/19700101000001Z.log")

    test.write_pre("src/versions.txt", "versions 2..")
    test.backup(offset=3, allow_error=True)
    assert os.path.exists("dst/.dfb/logs/19700101000003Z.log")


def test_dst_compare_and_dst_renames():
    # Test and verify using local vs destination attributes
    test = testutils.Tester(name="dst_attributes")

    # ## Compare
    #
    # See how comparisons go with and without reusing hashes

    vq = ["-v"]

    # +
    test.config["dst_list_rclone_flags"] = ["--fast-list"]

    test.config["compare"] = "hash"
    test.config["dst_compare"] = "hash"
    test.write_config()
    # -

    test.write_pre("src/file.txt", "file", dt=-30)
    back = test.backup(*vq, offset=1)

    test.write_pre("src/file.txt", "file", dt=-28)
    back = test.backup(*vq, offset=3)
    assert back.new + back.modified == []  # No transfer

    test.write_pre("src/file.txt", "file", dt=-26)
    test.backup("--refresh", *vq, offset=5)
    assert back.new + back.modified == []  # No transfer

    test.config["dst_compare"] = "mtime"
    test.write_config()

    # This should still not transfer since we are src-to-src compare
    test.write_pre("src/file.txt", "file", dt=-24)
    back = test.backup(*vq, offset=7)
    assert back.new + back.modified == []  # No transfer

    # This should transfer since we are src-to-dst compare
    test.write_pre("src/file.txt", "file", dt=-22)
    back = test.backup("--refresh", "--no-refresh-use-snapshots", "-v", offset=9)
    assert back.new + back.modified == ["file.txt"]  # Transfer

    # Side Test: Make sure dst_list_rclone_flags got set. Look for --fast-list
    log = test.logs[-1][0]
    assert re.search("rclone call.*--fast-list", log)

    # This again NOT transfer since we are back to src-to-src
    test.write_pre("src/file.txt", "file", dt=-20)
    back = test.backup(*vq, offset=11)
    assert back.new + back.modified == []  # No transfer

    # ## Moves

    # +
    test.config["compare"] = "mtime"
    test.config["dst_compare"] = "mtime"

    test.config["renames"] = "hash"
    test.config["dst_renames"] = "hash"
    test.write_config()
    # -

    test.write_pre("src/mv1.txt", "file", dt=-30)
    back = test.backup(*vq, offset=13)

    test.move("src/mv1.txt", "src/mv2.txt")
    back = test.backup(*vq, offset=15)
    assert len(back.moves) == 1

    test.move("src/mv2.txt", "src/mv3.txt")
    back = test.backup("--refresh", *vq, offset=17)
    assert len(back.moves) == 1

    test.config["dst_renames"] = "mtime"
    test.write_config()

    # +
    # This should still track the move because we are src-to-src
    test.move("src/mv3.txt", "src/mv4.txt")
    back = test.backup(*vq, offset=19)

    assert len(back.moves) == 1
    assert (
        "Compare 'mv4.txt' with attrib = 'hash'. MATCH"
        in test.logs[-1][0] + test.logs[-1][1]
    )

    # +
    # This should still track the move from mtime because we are src-to-dst
    test.move("src/mv4.txt", "src/mv5.txt")
    back = test.backup("--refresh", "--no-refresh-use-snapshots", *vq, offset=21)

    assert len(back.moves) == 1
    assert (
        "Compare 'mv5.txt' with attrib = 'mtime'. MATCH"
        in test.logs[-1][0] + test.logs[-1][1]
    )
    # -

    test.config["dst_renames"] = False
    test.write_config()

    test.move("src/mv5.txt", "src/mv6.txt")
    back = test.backup(*vq, offset=23)
    assert len(back.moves) == 1
    assert (
        "Compare 'mv6.txt' with attrib = 'hash'. MATCH"
        in test.logs[-1][0] + test.logs[-1][1]
    )

    test.move("src/mv6.txt", "src/mv7.txt")
    back = test.backup("--refresh", "--no-refresh-use-snapshots", *vq, offset=25)
    # Disabled. Check it
    assert len(back.moves) == 0
    assert back.new == ["mv7.txt"]


def test_restore_error():
    test = testutils.Tester(name="restore_error")

    test.write_config()

    test.write_pre("src/file1.txt", "file1")
    test.write_pre("src/sub/file2.txt", "file2")
    test.backup("-q", offset=1)

    test.write_pre("src/file3.txt", "file3")
    test.write_pre("src/sub/file2.txt", "file2.")
    test.backup("-q", offset=3)

    # +
    test.call("restore-file", "sub/file2.txt", "testfile.txt", "--to", "-q")
    assert test.read("testfile.txt") == "file2."

    test.call(
        "restore-file",
        "sub/file2.txt",
        "testfile.txt",
        "--to",
        "--no-check",
        "--at",
        "u2",
        "-q",
    )
    assert test.read("testfile.txt") == "file2"
    # -

    # While we are at it, restore to the source to test that
    test.call("restore-dir", "@src/new", "--no-check")

    assert {dict(i)["apath"] for i in test.local_files()} == {
        "file1.txt",
        "file3.txt",
        "new/file1.txt",
        "new/file3.txt",
        "new/sub/file2.txt",
        "sub/file2.txt",
    }

    test.backup(offset=5)

    os.unlink("dst/sub/file2.19700101000001.txt")

    test.call(
        "restore-file", "sub/file2.txt", "testfile.txt", "--to", "-q", "--no-check"
    )
    assert test.read("testfile.txt") == "file2."

    # +
    test.call(
        "restore-file",
        "sub/file2.txt",
        "testfile.txt",
        "--to",
        "--no-check",
        "--at",
        "u2",
    )  # must not have -q

    log = test.logs[-1][0]
    assert "ERROR: Could not restore 'sub/file2.19700101000001.txt'." in log
    assert "At least one restore did not work" in log
    assert test.read("testfile.txt") != "file2"  # What is should be
    assert test.read("testfile.txt") == "file2."  # from before
    # -

    test.call("restore-dir", "@src/neww", "--no-check", "--at", "u2")
    assert "ERROR: Could not restore 'sub/file2.19700101000001.txt'." in log
    assert "At least one restore did not work." in log

    test.call("versions", "sub/file2.txt")
    test.call("refresh")
    test.call(
        "restore-file",
        "sub/file2.txt",
        "testfile.txt",
        "--to",
        "--no-check",
        "--at",
        "u2",
    )  # must not have -q
    log = test.logs[-1][0]
    assert "ERROR: Could not find 'sub/file2.txt' at the specified time" in log

    # Test some restores with no file


@pytest.mark.parametrize("mode", ["size", "mtime", "hash"])
def test_false_negs_compare(mode):
    test = testutils.Tester(name="false_negs_compare")

    compare = mode

    test.config["compare"] = compare
    test.write_config()

    # +
    test.write_pre("src/no_size_change.txt", "123")

    test.write_pre("src/no_size-mtime_change.txt", "1234")
    t0 = os.stat("src/no_size-mtime_change.txt")

    test.write_pre("src/touch.txt", "touch me")

    test.write_pre("src/change_size.txt", "ABCD")
    t1 = os.stat("src/change_size.txt")

    test.backup(offset=1)

    # +
    test.write_post("src/no_size_change.txt", "321")

    test.write_post("src/no_size-mtime_change.txt", "4321")
    os.utime("src/no_size-mtime_change.txt", (t0.st_atime, t0.st_mtime))

    test.write_post("src/touch.txt", "touch me")

    test.write_post("src/change_size.txt", "ABC")
    os.utime("src/change_size.txt", (t1.st_atime, t1.st_mtime))

    back = test.backup("-v", offset=3)

    if compare == "size":
        assert set(back.modified) == {"change_size.txt"}
    elif compare == "mtime":
        assert set(back.modified) == {
            "change_size.txt",
            "no_size_change.txt",
            "touch.txt",
        }
    elif compare == "hash":
        assert set(back.modified) == {
            "change_size.txt",
            "no_size_change.txt",
            "no_size-mtime_change.txt",
        }


def test_missing_ref():
    test = testutils.Tester(name="missing_ref")

    test.write_config()

    # +
    test.write_pre("src/file1.txt", "123")
    test.write_pre("src/file2.txt", "ABCD")

    test.backup(offset=1)

    # +
    test.move("src/file1.txt", "src/fileONE.txt")
    test.write_pre("src/file2.txt", "ABCDE")

    test.backup(offset=3)
    # -

    os.unlink("dst/file1.19700101000001.txt")

    test.call("ls")

    test.call("restore-file", "file2.txt", "-", "-q")

    # +
    test.call("restore-file", "fileONE.txt", "-")
    log = test.logs[-1][0]
    assert "ERROR: Could not restore 'file1.19700101000001.txt'" in log
    assert "At least one restore did not work" in log

    # Same thing but catch the error
    try:
        test.call("restore-file", "fileONE.txt", "-", "-v")  # -v will make it error
        assert False
    except ValueError:
        pass
    # -

    test.call("refresh", "--no-use-snapshot")
    out = test.ls("-q")

    log = "\n".join(l[0] for l in test.logs[-2:])
    assert (
        "WARNING: File 'fileONE.19700101000003R.txt' "
        "references 'file1.19700101000001.txt' but it is missing. "
        "Will just be treated as deleted"
    ) in log

    assert "file2.txt" in out

    out = test.ls("-d")
    assert "fileONE.txt (DEL)" in out


def test_override():
    """
    Test overrides including pre,post values.

    Do this simply by adding some print statements
    """
    test = testutils.Tester(name="override")
    test.write_config()

    test.write_pre("src/file1.txt", "1")
    test.backup("-o", "print('twice?')", offset=1)
    log = test.logs[-1][0]
    assert log.count("twice?") == 2

    test.write_pre("src/file1.txt", "12")
    test.backup(
        "-o",
        dedent(
            """\
        if pre:
            print('one PRE')
            try:
                print(f"{newval = }")
                raise ValueError()
            except NameError:
                newval = 10
                print('Set newval')
        if post:
            print('one POST')
            print(f"{newval = }")
        """
        ),
        offset=3,
    )
    log = test.logs[-1][0]
    assert log.count("one PRE") == 1
    assert log.count("one POST") == 1
    assert log.count("Set newval") == 1
    assert log.count("newval = 10") == 1


def test_subdirs():
    test = testutils.Tester(name="subdirs")

    filters = [
        "- *.exc",  # Wildcard
        "- general_exc.txt",  # General
        "- /sub/specific_exc.txt",  # specific. Anchored
        "- /sub2/**",  # General
    ]

    test.config["filter_flags"] = []
    for filt in filters:
        test.config["filter_flags"].extend(["--filter", filt])

    test.write_config()

    test.write_pre("src/file1.txt", "1")
    test.write_pre("src/sub/file2.txt", "2")
    test.write_pre("src/sub/file3.exc", "3")
    test.write_pre("src/sub/general_exc.txt", "exc")
    test.write_pre("src/sub/specific_exc.txt", "exc")
    test.write_pre("src/file4.exc", "4")
    test.write_pre("src/sub2/file5.txt", "5")

    test.backup(offset=1)

    assert test.src_missing_in_dst(keys=("apath",)) == {
        frozenset({("apath", "sub/file3.exc")}),
        frozenset({("apath", "sub/specific_exc.txt")}),
        frozenset({("apath", "sub/general_exc.txt")}),
        frozenset({("apath", "file4.exc")}),
        frozenset({("apath", "sub2/file5.txt")}),
    }

    test.write_post("src/file1.txt", "1POST")
    test.write_post("src/sub/file2.txt", "2POST")

    back = test.backup("--subdir", "sub", offset=3)

    assert set(back.new) == {"sub/specific_exc.txt"}  # Broken specific filter
    assert set(back.modified) == {"sub/file2.txt"}  # No file1.txt

    back = test.backup(offset=5)

    assert set(back.modified) == {"file1.txt"}  # Now captures the mod
    assert set(back.deleted) == {
        "sub/specific_exc.txt"
    }  # removed since it is now filtered

    # Make sure we never made sub2
    assert not os.path.exists("dst/sub2")


def test_subdir_w_empty():
    """
    Regression for non-empty (or maybe even actually empty) subdirs w/o modified
    files showing as empty
    """
    test = testutils.Tester(name="subdir_w_empty")

    test.config["empty_directory_markers"] = True
    test.write_config()

    test.write_pre("src/file1.txt", "1")
    test.write_pre("src/sub0/file2.txt", "2")
    test.write_pre("src/sub0/sub1/file3.txt", "3")
    test.write_pre("src/sub0/sub2/file4.txt", "4")

    test.backup(offset=1)

    test.write_post("src/sub0/sub2/file4.txt", "four")
    os.makedirs("src/sub0/sub3")

    test.backup("--subdir", "sub0", offset=3)

    # Make sure the empty dir maker is created even at the subdir
    assert os.path.exists("dst/sub0/sub3/.dfbempty.19700101000003")

    # this is the regression. Notice these are relative to 'sub0'
    assert not os.path.exists("dst/" + "sub1/.dfbempty.19700101000003")
    assert not os.path.exists("dst/" + "sub2/.dfbempty.19700101000003")


@pytest.mark.parametrize("mode", ["link", "link-webdav", "copy", "skip"])
def test_symlinks(mode):
    test = testutils.Tester(name="symlinks", src="srcalias:")

    linkmode = mode
    args = []
    if mode == "link-webdav":
        linkmode = "link"
        webdav = testutils.WebDAV(path="dst").start()
        args = ["--override", f"dst = {webdav.remote!r}"]

    # test.config["links"] = linkmode
    if linkmode == "link":
        test.config["rclone_flags"] = ["--links"]
    elif linkmode == "copy":
        test.config["rclone_flags"] = ["--copy-links"]
    elif linkmode == "skip":
        test.config["rclone_flags"] = ["--skip-links"]

    test.config["upload_logs"] = False

    test.write_config()

    test.write("src/file1.txt", "File ONE")
    test.write("src/sub/file2.txt", "File 2")
    os.symlink("file1.txt", "src/link1.txt")
    os.symlink("sub/file2.txt", "src/link2.1.txt")
    os.symlink("file2.txt", "src/sub/link2.2.txt")
    Path("src/other/").mkdir(exist_ok=True, parents=True)
    os.symlink("../file1.txt", "src/other/backfile1.txt")
    os.symlink("../sub/file2.txt", "src/other/backfile2.txt")

    test.backup(*args, offset=1)
    test.call("restore", "res", *args)
    test.call("refresh")

    if mode == "link":
        assert os.readlink("dst/link1.19700101000001.txt") == "file1.txt"
        assert os.readlink("dst/link2.1.19700101000001.txt") == "sub/file2.txt"
        assert os.readlink("dst/other/backfile1.19700101000001.txt") == "../file1.txt"
        assert (
            os.readlink("dst/other/backfile2.19700101000001.txt") == "../sub/file2.txt"
        )
        assert os.readlink("dst/sub/link2.2.19700101000001.txt") == "file2.txt"

        # Restore test. Do not need to do them all. Just make sure it's a link
        assert os.readlink("res/link1.txt") == "file1.txt"

    elif mode == "link-webdav":
        assert {dict(a)["apath"] for a in test.remote_snapshot()} == {
            "file1.txt",
            "link1.txt.rclonelink",
            "link2.1.txt.rclonelink",
            "other/backfile1.txt.rclonelink",
            "other/backfile2.txt.rclonelink",
            "sub/file2.txt",
            "sub/link2.2.txt.rclonelink",
        }
        assert set(testutils.tree("dst")) == {
            "dst/file1.19700101000001.txt",
            "dst/link1.19700101000001.txt.rclonelink",
            "dst/link2.1.19700101000001.txt.rclonelink",
            "dst/other/backfile1.19700101000001.txt.rclonelink",
            "dst/other/backfile2.19700101000001.txt.rclonelink",
            "dst/sub/file2.19700101000001.txt",
            "dst/sub/link2.2.19700101000001.txt.rclonelink",
        }

        assert test.read("dst/link1.19700101000001.txt.rclonelink") == "file1.txt"
        assert test.read("dst/link2.1.19700101000001.txt.rclonelink") == "sub/file2.txt"
        assert (
            test.read("dst/other/backfile1.19700101000001.txt.rclonelink")
            == "../file1.txt"
        )
        assert (
            test.read("dst/other/backfile2.19700101000001.txt.rclonelink")
            == "../sub/file2.txt"
        )
        assert test.read("dst/sub/link2.2.19700101000001.txt.rclonelink") == "file2.txt"
        assert len(test.tree_sha1s("dst")) == 7

        # Restore test. Do not need to do them all. Just make sure it's a link
        assert os.readlink("res/link1.txt") == "file1.txt"

    elif mode == "copy":
        assert {dict(a)["apath"] for a in test.remote_snapshot()} == {
            "file1.txt",
            "link1.txt",
            "link2.1.txt",
            "other/backfile1.txt",
            "other/backfile2.txt",
            "sub/file2.txt",
            "sub/link2.2.txt",
        }
        assert set(testutils.tree("dst")) == {
            "dst/file1.19700101000001.txt",
            "dst/link1.19700101000001.txt",
            "dst/link2.1.19700101000001.txt",
            "dst/other/backfile1.19700101000001.txt",
            "dst/other/backfile2.19700101000001.txt",
            "dst/sub/file2.19700101000001.txt",
            "dst/sub/link2.2.19700101000001.txt",
        }
        assert not any(os.path.islink(f) for f in testutils.tree("dst"))
        assert len(test.tree_sha1s("dst")) == 2

        assert not os.path.islink("res/link1.txt")

    elif mode == "skip":
        assert {dict(a)["apath"] for a in test.remote_snapshot()} == {
            "file1.txt",
            "sub/file2.txt",
        }
        assert set(testutils.tree("dst")) == {
            "dst/file1.19700101000001.txt",
            "dst/sub/file2.19700101000001.txt",
        }
        assert len(test.tree_sha1s("dst")) == 2

        assert not os.path.exists("res/link1.txt")

    # Make sure a second copy doesn't mess with this
    back2 = test.backup(offset=3)
    assert back2.modified == back2.moves == back2.deleted == []

    back2 = test.backup("--refresh", offset=5)
    assert back2.modified == back2.moves == back2.deleted == []


def test_symlinks_in_union():
    """
    This is a (regression) test for links in a union drive
    """
    test = testutils.Tester(name="symlink_in_union")
    test.config["links"] = "link"
    test.config["concurrency"] = 1
    test.config["upload_logs"] = False

    os.makedirs("empty")
    empty = os.path.abspath("empty")
    src = test.config["src"]

    env = test.config["rclone_env"]
    env["RCLONE_UNION_UPSTREAMS"] = shlex.join([src, empty])

    test.config["src"] = ":union:"
    test.write_config()

    test.write("src/file1.txt", "File ONE")
    os.symlink("file1.txt", "src/link1.txt")
    test.backup("-v", offset=1)


def test_snapshots():
    test = testutils.Tester(name="snapshots")

    test.write_config()

    test.write_pre("src/versions.txt", "versions 1")
    test.write_pre("src/new1.txt", "do not touch")
    test.write_pre("src/new1_del3.txt", "new at 1. Del at 3")

    test.backup(offset=1)

    test.write_post("src/versions.txt", "versions 3.")
    os.unlink("src/new1_del3.txt")
    test.write_pre("src/new3_mv5.txt", "new at 3. mv at 5")

    test.backup(offset=3)

    test.write_pre("src/versions.txt", "versions 5..")
    test.move("src/new3_mv5.txt", "src/MOVED/new3_mv5 DONE.txt")

    test.backup(offset=5)

    # Call for snapshots. Exports should be cumulative
    test.call("snapshot", "--output", "1.jsonl", "--only", "u1", "--deleted")
    test.call("snapshot", "--output", "3.jsonl", "--only", "u3", "--deleted")
    test.call("snapshot", "--output", "5.jsonl", "--only", "u5", "--deleted")
    test.call("snapshot", "--output", "e1.jsonl", "--before", "u1", "--export")
    test.call("snapshot", "--output", "e3.jsonl", "--before", "u3", "--export")
    test.call("snapshot", "--output", "e5.jsonl", "--before", "u5", "--export")

    keys = ["rpath", "apath", "timestamp", "size"]
    cumupl = set()
    for uz in [1, 3, 5]:
        with open(f"{uz}.jsonl") as fp:
            cli = [json.loads(line) for line in fp]
            cli = {frozenset((k, f[k]) for k in keys) for f in cli}

        with open(f"e{uz}.jsonl") as fp:
            exp = [json.loads(line) for line in fp]
            exp = {frozenset((k, f[k]) for k in keys) for f in exp}

        with gz.open(f"dst/.dfb/snapshots/1970/01/1970010100000{uz}Z.jsonl.gz") as fp:
            upl = [json.loads(line) for line in fp]
            upl = {frozenset((k, f[k]) for k in keys) for f in upl}
            cumupl.update(upl)

        assert cli == upl
        assert exp == cumupl

    # Test with import
    test.call("snapshot", "--output", "oe1.jsonl", "--only", "u1", "--export")
    test.call("snapshot", "--output", "oe3.jsonl", "--only", "u3", "--export")
    test.call("snapshot", "--output", "oe5.jsonl", "--only", "u5", "--export")

    # w/ reset
    test.call("advanced", "dbimport", "--reset", "oe1.jsonl", "oe3.jsonl", "oe5.jsonl")
    test.call("snapshot", "--output", "new.jsonl", "--export")
    with open("new.jsonl") as fp:
        exp = [json.loads(line) for line in fp]
        exp = {frozenset((k, f[k]) for k in keys) for f in exp}
    assert exp == cumupl

    # w/ reset then each its own all. Should be the same
    test.call("advanced", "dbimport", "--reset")  # reset
    test.call("advanced", "dbimport", "oe1.jsonl")
    test.call("advanced", "dbimport", "oe3.jsonl")
    test.call("advanced", "dbimport", "oe5.jsonl")
    test.call("snapshot", "--output", "new.jsonl", "--export")
    with open("new.jsonl") as fp:
        exp = [json.loads(line) for line in fp]
        exp = {frozenset((k, f[k]) for k in keys) for f in exp}
    assert exp == cumupl

    # w/ reset but in a directory
    Path("tmpdir").mkdir()
    shutil.copy2("oe1.jsonl", "tmpdir")
    shutil.copy2("oe3.jsonl", "tmpdir")
    shutil.copy2("oe5.jsonl", "tmpdir")
    test.call("advanced", "dbimport", "--reset", "--dirs", "tmpdir")
    test.call("snapshot", "--output", "new.jsonl", "--export")
    with open("new.jsonl") as fp:
        exp = [json.loads(line) for line in fp]
        exp = {frozenset((k, f[k]) for k in keys) for f in exp}
    assert exp == cumupl

    # w/o reset
    test.call("advanced", "dbimport", "oe1.jsonl", "oe3.jsonl", "oe5.jsonl")
    test.call("snapshot", "--output", "new.jsonl", "--export")
    with open("new.jsonl") as fp:
        exp = [json.loads(line) for line in fp]
        exp = {frozenset((k, f[k]) for k in keys) for f in exp}
    assert exp == cumupl

    ## Now prune things
    test.call("prune", "u7", offset=7)
    shutil.copy2(
        "dst/.dfb/snapshots/1970/01/19700101000007Z.jsonl.gz", "prune.jsonl.gz"
    )

    with gz.open("prune.jsonl.gz", "rt") as fp:
        for line in fp:
            line = json.loads(line)
            assert line["_V"] == 1
            assert line["_action"] == "prune"

    # reset, import again then apply.
    # Also test the uploads
    test.call(
        "advanced",
        "dbimport",
        "--reset",
        "oe1.jsonl",
        "oe3.jsonl",
        "oe5.jsonl",
        "prune.jsonl.gz",
        "--upload",
        offset=1_234_567_890,
    )
    log = test.logs[-1][0]
    assert "Imported 0 files and will prune 4" in log
    assert "Imported 3 files and will prune 4" not in log, "should not have mixed"
    assert "Pruned 4 files from all exports" in log

    test.call("snapshot", "--output", "new.jsonl", "--export")
    with open("new.jsonl") as fp:
        exp = [json.loads(line) for line in fp]
        exp = {frozenset((k, f[k]) for k in keys) for f in exp}
    assert exp != cumupl, "Should NOT match with the exports because it was pruned!"

    # test that the files got uploaded. Should NOT be compressed unless already
    for ff in ["0.oe1.jsonl", "1.oe3.jsonl", "2.oe5.jsonl", "3.prune.jsonl.gz"]:
        assert (test.pwd / "dst/.dfb/snapshots/2009/02/20090213233130Z" / ff).exists()


def test_missing_hashes():
    test = testutils.Tester(name="missing_hashes")
    test.config["compare"] = "hash"
    test.write_config()

    test.write_pre("src/same_size.txt", "versions 1")
    test.backup(offset=1)

    try:
        from dfb import _FAIL

        _FAIL.add("missing_hashes")

        test.write_post("src/same_size.txt", "versions 2")  # Same size!
        test.backup(offset=3)
        log = test.logs[-1][0]
        assert "WARNING: Missing hashes on source and/or dest" in log
        assert "Reverting to 'size' only" in log

        # Make sure it didn't back it up
        assert not os.path.exists("dst/same_size.19700101000003.txt")

        # This one should work even with missing hashes
        test.backup("-o", "compare='mtime'", offset=5)
        assert os.path.exists("dst/same_size.19700101000005.txt")

        # Do it again but this should fail
        # '-v' to get an error
        try:
            test.backup("-v", "-o", "error_on_missing_hash = True", offset=7)
            assert False
        except NoCommonHashError:
            pass

    finally:
        _FAIL.remove("missing_hashes")


@pytest.mark.parametrize("metadata", [True, False])
def test_metadata(metadata):
    test = testutils.Tester(name="metadata")
    test.config["metadata"] = metadata

    # Make sure macOS isn't cloning as that won't test the metadata.
    test.config["rclone_flags"] = test.config.get("rclone_flags", [])
    test.config["rclone_flags"].append("--local-no-clone")
    test.write_config()

    uu = 4, 6, 7
    gg = oo = tuple(range(8))
    for u, g, o in itertools.product(uu, gg, oo):
        filename = f"{u}{g}{o}.txt"
        perm = int(f"{u}{g}{o}", 8)
        test.write_pre(f"src/{filename}", filename)
        os.chmod(f"src/{filename}", perm)

    test.backup(offset=1)
    test.call("restore", "--at", "u1.1", str(test.pwd / f"res1"))

    # This is slow so I comment it out but it has been tested and I can come back to it.
    # restore_dir = str(test.pwd / "res2")
    # restore_script = str(test.pwd / "res2.sh")
    # test.call("restore","--at","u1.1",restore_dir,"--shell-script",restore_script)
    # subprocess.check_call(["bash", restore_script])

    # Verify the src too just to be complete
    n = 0
    correct = 0
    vdirs = ["src", "dst", "res1"]  # ,'res2']
    for vdir in vdirs:
        for file in os.listdir(vdir):
            gold = file.split(".", 1)[0]
            mode = os.stat(os.path.join(vdir, file)).st_mode
            correct += int(oct(mode).endswith(gold))
            n += 1
    print(f"verified {n}. Correct {correct}")

    if metadata:
        assert correct == n
    else:
        assert correct < n


def test_dump():
    test = testutils.Tester(name="dump")

    test.config["metadata"] = False  # Issues with atime due to run time
    test.config["renames"] = "mtime"
    test.write_config()

    def comp(t):
        """Compare results. Order doesn't matter"""
        with open(f"t{t}.jsonl") as fp:
            dump = {testutils.dict2frozen(json.loads(l)) for l in fp}
        with gz.open(f"dst/.dfb/snapshots/1970/01/197001010000{t:02d}Z.jsonl.gz") as fp:
            snap = {testutils.dict2frozen(json.loads(l)) for l in fp}
        return dump == snap

    test.write_pre("src/new.txt", "new!")
    test.write_pre("src/modify.txt", "modify")
    test.write_pre("src/move_by_ref.txt", "will move by ref")
    test.write_pre("src/move_by_ref_then_copy.txt", "will move by ref then copy")
    test.write_pre("src/move_by_copy.txt", "will move by copy")
    test.write_pre("src/delete.txt", "delete")

    test.backup("--dump", "t1.jsonl", offset=1)
    test.backup(offset=1)
    assert comp(1)

    test.write_post("src/modify.txt", "modify.")
    shutil.move("src/move_by_ref.txt", "src/moved_by_ref.txt")
    shutil.move("src/move_by_ref_then_copy.txt", "src/moved_by_ref_then_copy.txt")
    os.unlink("src/delete.txt")

    test.backup("--dump", "t3.jsonl.gz", offset=3)  # Just checking the compression
    test.backup("--dump", "t3.jsonl.xz", offset=3)  # Just checking the compression
    test.backup("--dump", "t3.jsonl", offset=3)
    test.backup(offset=3)
    assert comp(3)

    test.write_post("src/modify.txt", "modify..")
    shutil.move("src/moved_by_ref_then_copy.txt", "src/moved_by_ref_now_copy.txt")
    shutil.move("src/move_by_copy.txt", "src/moved_by_copy.txt")

    test.backup("--dump", "t5.jsonl", "-o", "rename_method = 'copy'", offset=5)
    test.backup("-o", "rename_method = 'copy'", offset=5)
    assert comp(5)

    with (
        open("t3.jsonl", "rt") as r,
        gz.open("t3.jsonl.gz", "rt") as g,
        xz.open("t3.jsonl.xz", "rt") as x,
    ):
        assert r.read() == g.read() == x.read()

    test.call("prune", "u4", "--dump", "t7.jsonl", offset=7)
    test.call("prune", "u4", offset=7)
    assert comp(7)


def test_auto():
    """
    WARNING -- these may have to be updated in newer versions of rclone if
               any of the remotes get new capabilities!
    """
    autos = [
        *["compare", "dst_compare"],
        *["renames", "dst_renames"],
        *["get_modtime", "get_hashes"],
    ]

    test = testutils.Tester(name="auto_settings")

    test.config |= {k: "auto" for k in autos}

    config = test.write_config()
    assert all(getattr(config, k, False) == "auto" for k in autos)

    config._set_auto()

    assert {k: getattr(config, k, None) for k in autos} == {
        "compare": "mtime",
        "dst_compare": "mtime",
        "renames": "mtime",
        "dst_renames": "mtime",
        "get_modtime": True,
        "get_hashes": False,
    }


def test_min_size():
    test = testutils.Tester(name="min_size")

    test.config["min_rename_size"] = "0.000005 mb"  # 5 bytes
    test.write_config()

    test.write_pre("src/small0.txt", "0")
    test.write_pre("src/large0.txt", "0123456789")

    test.backup(offset=1)

    shutil.move("src/small0.txt", "src/small1.txt")
    shutil.move("src/large0.txt", "src/large1.txt")

    test.backup("-v", offset=3)

    assert test.read("dst/small1.19700101000003.txt") == "0", "moved"
    assert not os.path.exists("dst/small1.19700101000003R.txt")

    assert json.loads(test.read("dst/large1.19700101000003R.txt")) == {
        "ver": 2,
        "rel": "large0.19700101000001.txt",
    }
    assert not os.path.exists("dst/large1.19700101000003.txt")

    # Make it call copy
    test.config["min_rename_size"] = "5 Pib"
    test.write_config()

    shutil.move("src/small1.txt", "src/small2.txt")
    shutil.move("src/large1.txt", "src/large2.txt")

    test.backup("-v", offset=5)

    assert test.read("dst/small2.19700101000005.txt") == "0"
    assert not os.path.exists("dst/small2.19700101000005R.txt")

    assert test.read("dst/large2.19700101000005.txt") == "0123456789"
    assert not os.path.exists("dst/large2.19700101000005R.txt")


def test_push_snapshots():
    """
    Test pushing snapshot files to the destination

    Note that testing for dbimport is done in tha
    """
    test = testutils.Tester(name="push_snap")
    test.write_config()

    test.write_pre("src/file.txt", "0")

    # Write a .jsonl file that mimics being left from last time. Should also be compressed
    tfile = test.pwd / "cache/DFB/test_push_snap.snap" / "2022/06" / "test.jsonl"
    tfile.parent.mkdir(exist_ok=True, parents=True)
    tfile.write_text("this is a dummy file")

    test.backup(offset=1)

    # Make sure it got backed up
    qfile = test.pwd / "dst/.dfb/snapshots/2022/06/test.jsonl.gz"

    assert not tfile.exists(), "didn't get moved"
    assert qfile.exists()
    assert not qfile.with_suffix("").exists(), "didn't get compressed"
    assert gz.open(str(qfile), "rt").read() == "this is a dummy file", "not compressed"


def test_empty_dirs():
    test = testutils.Tester(name="empty_dirs")

    test.config["filter_flags"] = ["--filter", "- *.exc"]
    test.config["empty_directory_markers"] = True

    test.write_config()

    os.makedirs("src/empty")
    os.makedirs("src/hassub/subdir/")

    test.backup("-v", offset=1)

    assert os.path.exists("dst/empty/.dfbempty.19700101000001")
    assert os.path.exists("dst/hassub/subdir/.dfbempty.19700101000001")

    shutil.move("src/hassub/subdir/", "src/hassub/subdir_moved")
    test.write_pre("src/empty/file.txt", "file")

    test.backup("-v", offset=3)

    # Test both now and past
    assert test.tree("--at", "u2") == dedent(
        """\
        /
        ├── empty/
        │   └── .dfbempty
        └── hassub/
            └── subdir/
                └── .dfbempty
        """
    )
    assert test.tree() == dedent(
        """\
        /
        ├── empty/
        │   └── file.txt
        └── hassub/
            └── subdir_moved/
                └── .dfbempty
        """
    )

    files = (os.path.relpath(f, "dst") for f in testutils.tree("dst", hidden=True))
    files = {f for f in files if not f.startswith(".dfb/")}

    assert {
        "empty/.dfbempty.19700101000001",
        "empty/.dfbempty.19700101000003D",
        "empty/file.19700101000003.txt",
        "hassub/subdir/.dfbempty.19700101000001",
        "hassub/subdir/.dfbempty.19700101000003D",
        "hassub/subdir_moved/.dfbempty.19700101000003",
    } == files

    # Now the empty dir markers shoudl go away when not tracking
    test.backup("-v", "--override", "empty_directory_markers = False", offset=5)
    assert os.path.exists("dst/hassub/subdir_moved/.dfbempty.19700101000005D")

    # and back
    res = test.backup("-v", offset=7)
    assert os.path.exists("dst/hassub/subdir_moved/.dfbempty.19700101000007")

    # restores
    test.call("restore", "restore_curr")
    assert {
        "restore_curr/empty/file.txt",
        "restore_curr/hassub/subdir_moved/.dfbempty",
    } == set(testutils.tree("restore_curr/", hidden=True))

    test.call("restore", "restore2", "--before", "u2")
    assert {
        "restore2/empty/.dfbempty",
        "restore2/hassub/subdir/.dfbempty",
    } == set(testutils.tree("restore2/", hidden=True))


def test_refresh_no_snapshots():
    test = testutils.Tester(name="refresh_no_snapshots")
    test.write_config()

    test.write_pre("src/A.txt", "A")
    test.write_pre("src/B.txt", "B")
    test.backup("-v", offset=1)

    test.write_pre("src/A.txt", "AA")
    test.backup("-v", offset=3)

    test.write_pre("src/B.txt", "BB")
    test.backup("-v", offset=5)

    shutil.rmtree("dst/.dfb/")
    test.call("refresh", "-vv")  # This used to throw errors on pulling the snapshot

    log = test.logs[-1][0]
    assert "Unable to load snapshots from remote" in log


if __name__ == "__main__":
    test_main("reference")
    #     test_main("copy")
    #     test_shell()
    #     test_log_upload()
    #     test_dst_compare_and_dst_renames(False)
    #     test_restore_error()
    #     for mode in ["size", "mtime", "hash":
    #         test_false_negs_compare(mode)
    #     test_missing_ref()
    #     test_override()
    #     test_subdirs()
    #     test_subdir_w_empty()
    #     test_symlinks("link")
    #     test_symlinks("link-webdav")
    #     test_symlinks('copy')
    #     test_symlinks("skip")
    #     test_symlinks_in_union()
    #     test_snapshots()
    #     test_missing_hashes()
    #     test_metadata(True)
    #     test_metadata(False)
    #     test_dump()
    #     test_auto()
    #     test_min_size()
    #     test_push_snapshots()
    #     test_empty_dirs()
    #     test_refresh_no_snapshots()
    print("=" * 50)
    print(" All Passed ".center(50, "="))
    print("=" * 50)

    #     #test_shell_scripts('reference')
    #     #test_shell_scripts('copy')
