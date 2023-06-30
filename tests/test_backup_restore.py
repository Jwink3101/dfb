#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, shutil
from pathlib import Path
import gzip as gz
import re
import subprocess
import json
import itertools
from textwrap import dedent

p = os.path.abspath("../")
if p not in sys.path:
    sys.path.insert(0, p)

from dfb.cli import cli
from dfb.backup import NoCommonHashError

# Local
import testutils

# testing
import pytest

_r = repr


@pytest.mark.parametrize("rename_method", ["reference", "copy"])
def test_main(rename_method):
    import json

    test = testutils.Tester(name="main")

    test.config["renames"] = "mtime"
    test.config["filter_flags"] = ["--filter", "- *.exc"]
    test.config["rename_method"] = rename_method

    # # Main Test -- Tracking

    test.write_config()

    # +
    test.write_pre("src/versions.txt", "versions 1.")
    test.write_pre(
        "src/unįçôde, spaces, symb°ls (!@#$%^&*) in €‹›ﬁﬂ‡°·—±", "did it work?"
    )
    test.write_pre("src/untouched.txt", "Do not modify")
    test.write_pre("src/mod_same-size.txt", "modify but not change size")
    test.write_pre("src/mod_diff_size.txt", "modify and change size")
    test.write_pre(
        "src/mod_same-size-mtime.txt", "modify but not change size OR modTime"
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
        conditions=[("apath = ?", "sub2/moved.txt")]
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
    test.call("snapshot", "--refresh", "--output", "B.jsonl")

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
    test.call("ls", "-vv", "--refresh")
    log = test.logs[-1][0]
    if rename_method == "reference":
        assert "Reference 'moved_again.19700101000005R.txt' is v2" in log
        with open("dst/moved_again.19700101000005R.txt", "wt") as fp:
            fp.write("sub/move.19700101000001.txt")
        test.call("ls", "-vv", "--refresh")
        log = test.logs[-1][0]
        assert "Reference 'moved_again.19700101000005R.txt' is v1 (implied)" in log

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

    test.call("ls", "-d", "--after", "u4", "--no-header")
    items = {l.strip() for l in test.logs[-1][0].split("\n") if l.strip()}
    assert items == {"versions.txt", "moved_again.txt", "sub2/"}

    cli(["init", "new.py"])
    cli(["init", "new.py"])
    cli(["init", "new.py", "--force-overwrite"])

    test.config["rclone_env"]["RCLONE_CONFIG_PASS"] = "secret"
    test.write_config()
    r = _r(test.config_obj)
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
        import os
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
    assert ".pre.shell: $ echo PRESHELL" in out
    assert ".pre.shell: $ echo PWD = $PWD" in out
    assert ".pre.shell: $ echo CONFIGDIR = $CONFIGDIR" in out
    assert ".pre.shell.out: PRESHELL" in out
    assert ".pre.shell.out: PWD = /" in out
    assert ".pre.shell.out: CONFIGDIR = /" in out

    assert (
        r"""post.shell: ['python', '-c', 'print("Post Shell")\nimport os\nprint(f"{os.getcwd() = }")\nsys.exit(10)']"""
        in out
    )
    assert "post.shell.out: Post Shell" in out
    assert "post.shell.out: os.getcwd() = '/" in out
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
    assert "post.shell.out: os.getcwd() = '/" in out
    assert "post.shell.out: os.environ.get('SHELL_TEST','FAIL') = 'SUCCESS'" in out
    assert "post.shell.out: os.environ.get('CONFIGDIR','FAIL') = '/" in out


@pytest.mark.parametrize("upload_logs", [True, False])
def test_log_upload(upload_logs):
    test = testutils.Tester(name="log_upload")

    test.config["upload_logs"] = upload_logs
    test.write_config()

    test.write_pre("src/versions.txt", "versions 1.")
    test.backup(offset=1)
    assert os.path.exists("dst/.dfb/logs/19700101000001Z.log") == upload_logs

    try:
        from dfb import _FAIL

        _FAIL.add("backup_transfer")

        test.write_pre("src/versions.txt", "versions 2..")
        test.backup(offset=3, allow_error=True)
        assert os.path.exists("dst/.dfb/logs/19700101000003Z.log") == upload_logs
    finally:
        _FAIL.remove("backup_transfer")


@pytest.mark.parametrize("reuse_hashes", ["mtime", False])
def test_dst_compare_and_dst_renames(reuse_hashes):
    # Test and verify using local vs destination attributes

    test = testutils.Tester(name="dst_attributes")

    # ## Compare
    #
    # See how comparisons go with and without reusing hashes

    vq = ["-v"]

    # +
    test.config["reuse_hashes"] = reuse_hashes
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
    back = test.backup("--refresh", "-v", offset=9)
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

    test.config["reuse_hashes"] = reuse_hashes

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
    # This should still track the move from mtime because we are src-to-dat
    test.move("src/mv4.txt", "src/mv5.txt")
    back = test.backup("--refresh", *vq, offset=21)

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
    back = test.backup("--refresh", *vq, offset=25)
    # Disabled. Check it
    assert len(back.moves) == 0
    assert back.new == ["mv7.txt"]


@pytest.mark.parametrize("rename_method", ["reference", "copy"])
def test_shell_scripts(rename_method):
    test = testutils.Tester(name="shell_scripts")

    test.config["renames"] = "hash"
    test.config["reuse_hashes"] = False
    test.config["hash_type"] = "sha1"
    test.config["rename_method"] = rename_method
    test.write_config()

    # +
    test.write_pre("src/do nothing.txt", "nothing")
    test.write_pre("src/will modify.txt", "modify me")
    test.write_pre("src/will move.txt", "move me")
    test.write_pre("src/will del.txt", "delete me")

    test.backup("--dry-run", offset=1)
    test.backup(offset=1)
    # -

    test.write_post("src/will modify.txt", "MODIFIED me")
    test.move("src/will move.txt", "src/has been move.txt")
    os.unlink("src/will del.txt")

    test.backup("--shell-script", "-", offset=3)
    test.backup("--shell-script", "run3.sh", offset=3)

    subprocess.call(["bash", "run3.sh"])
    run3 = test.read("run3.sh")

    if rename_method == "reference":
        # Script
        assert "## References (moves)" in run3
        assert run3.count("rcat") == 3  # 2 del + 1 move
        assert run3.count("printf") == 1

        # Process
        ref = test.read("dst/has been move.19700101000003R.txt")
        assert json.loads(ref) == {"ver": 2, "rel": "will move.19700101000001.txt"}

    else:
        # script
        assert "## Copies (moves)" in run3
        assert run3.count("rcat") == 2  # 2 del ONLY
        assert run3.count("printf") == 0
        assert run3.count("dst/will move.19700101000001.txt") == 1  # only one

        # process
        assert test.read("dst/has been move.19700101000003.txt") == "move me"

    # This won't make a difference in actual testing since there is no verification
    test.call("ls", "-ll")
    test.call("ls", "--no-header")
    items = {l.strip() for l in test.logs[-1][0].split("\n") if l.strip()}
    assert items == {
        "do nothing.txt",
        "will del.txt",
        "will modify.txt",
        "will move.txt",
    }  # These are wrong!

    test.call("ls", "--refresh", "--no-header")
    items = {l.strip() for l in test.logs[-1][0].split("\n") if l.strip()}
    assert items == {
        "do nothing.txt",
        "has been move.txt",
        "will modify.txt",
    }  # These are right
    test.call("ls", "-ll")


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
    test.call("versions", "sub/file2.txt", "--refresh")
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


@pytest.mark.parametrize("mode", ["size", "mtime", "hash", "hash_mtime"])
def test_false_negs_compare(mode):
    test = testutils.Tester(name="false_negs_compare")

    if mode == "hash_mtime":
        compare = "hash"
        reuse = "mtime"
    else:
        compare = mode
        reuse = False

    test.config["compare"] = compare
    test.config["reuse_hashes"] = reuse
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
        if reuse == "mtime":
            assert set(back.modified) == {"change_size.txt", "no_size_change.txt"}
        else:
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

    test.call("ls", "--refresh", "-q")
    log = test.logs[-1][0]
    assert (
        "WARNING: File 'fileONE.19700101000003R.txt' references 'file1.19700101000001.txt' but it is missing. Will just be treated as deleted"
        in log
    )
    assert "file2.txt" in log

    test.call("ls", "-d")
    log = test.logs[-1][0]
    assert "fileONE.txt (DEL)" in log


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


symlinks_modes = itertools.product(["link", "copy", "skip"], [True, False])


@pytest.mark.parametrize("mode,shell", symlinks_modes)
def test_symlinks(mode, shell):
    test = testutils.Tester(name="symlinks", src="srcalias:")  # Test with an alias!

    test.config["links"] = mode
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

    if not shell:
        test.backup(offset=1)
    else:
        test.backup("--shell-script", "back.sh")
        subprocess.call(
            """\
            chmod +x back.sh
            ./back.sh""",
            shell=True,
        )
        test.call("ls", "--refresh")  # Refresh b/c shell

    if mode == "link":
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
            "dst/link1.txt.19700101000001.rclonelink",
            "dst/link2.1.txt.19700101000001.rclonelink",
            "dst/other/backfile1.txt.19700101000001.rclonelink",
            "dst/other/backfile2.txt.19700101000001.rclonelink",
            "dst/sub/file2.19700101000001.txt",
            "dst/sub/link2.2.txt.19700101000001.rclonelink",
        }

        assert test.read("dst/link1.txt.19700101000001.rclonelink") == "file1.txt"
        assert test.read("dst/link2.1.txt.19700101000001.rclonelink") == "sub/file2.txt"
        assert (
            test.read("dst/other/backfile1.txt.19700101000001.rclonelink")
            == "../file1.txt"
        )
        assert (
            test.read("dst/other/backfile2.txt.19700101000001.rclonelink")
            == "../sub/file2.txt"
        )
        assert test.read("dst/sub/link2.2.txt.19700101000001.rclonelink") == "file2.txt"
        assert len(test.tree_sha1s("dst")) == 7
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

    # Make sure a second copy doesn't mess with this
    back2 = test.backup(offset=3)
    assert back2.modified == back2.moves == back2.deleted == []

    back2 = test.backup("--refresh", offset=5)
    assert back2.modified == back2.moves == back2.deleted == []


def test_symlinks_trick():
    test = testutils.Tester(name="symlinks_trick")
    test.config["links"] = "link"
    test.config["concurrency"] = 1
    test.config["upload_logs"] = False
    test.write_config()

    test.write("src/file1.txt", "File ONE")
    os.symlink("file1.txt", "src/link1.txt")
    test.write("src/trick.txt.rclonelink", "Not a link")

    test.backup("-v", offset=1)

    assert {dict(a)["apath"] for a in test.remote_snapshot()} == {
        "link1.txt.rclonelink",
        "file1.txt",
        "trick.txt.rclonelink",
    }
    assert test.read("dst/link1.txt.19700101000001.rclonelink").strip() == "file1.txt"
    assert test.read("dst/trick.txt.19700101000001.rclonelink").strip() == "Not a link"

    log = test.logs[-1][0]
    assert "trick.txt' could not be read. Treating as a file" in log


def test_keep_going_on_fail():
    """
    Make sure you keep running even if there is a failure
    """
    test = testutils.Tester(name="errs")
    test.write_config()

    test.write_pre("src/mod.txt", "mod me .")
    test.write_pre("src/del.txt", "del me ..")
    test.write_pre("src/mv.txt", "move me ...")

    test.backup(offset=1)

    test.write_post("src/mod.txt", "moded me .")
    os.unlink("src/del.txt")
    test.move("src/mv.txt", "src/moved.txt")

    try:
        from dfb import _FAIL

        _FAIL.update({"backup_transfer", "backup_reference", "backup_delete"})

        test.backup(offset=3)
    finally:
        _FAIL.difference_update(
            {"backup_transfer", "backup_reference", "backup_delete"}
        )

    # make sure it keeps going
    rem = {dict(a)["apath"] for a in test.remote_snapshot()}
    assert rem == {"mod.txt", "moved.txt"}
    assert test.read("dst/mod.19700101000003.txt") == "moded me ."


@pytest.mark.parametrize("upload", [True, False])
def test_snapshots(upload):
    test = testutils.Tester(name="snapshots")
    test.config["upload_snapshots"] = upload

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

    if not upload:
        assert not os.path.exists("dst/.dfb/snapshots/")
        return

    # Call for snapshots
    test.call("snapshot", "--output", "1.jsonl", "--only", "u1", "--deleted")
    test.call("snapshot", "--output", "3.jsonl", "--only", "u3", "--deleted")
    test.call("snapshot", "--output", "5.jsonl", "--only", "u5", "--deleted")

    keys = ["rpath", "apath", "timestamp", "size"]
    for uz in [1, 3, 5]:
        with open(f"{uz}.jsonl") as fp:
            cli = [json.loads(line) for line in fp]
            cli = {frozenset((k, f[k]) for k in keys) for f in cli}
        with open(f"dst/.dfb/snapshots/1970010100000{uz}Z.jsonl") as fp:
            upl = [json.loads(line) for line in fp]
            upl = {frozenset((k, f[k]) for k in keys) for f in upl}
        assert cli == upl


@pytest.mark.parametrize("reuse_hashes", [False, "mtime", "size"])
def test_reuse_hashes_method(reuse_hashes):
    test = testutils.Tester(name="reuse_hashes")
    test.config["reuse_hashes"] = reuse_hashes
    test.config["compare"] = "hash"
    test.write_config()

    test.write_pre("src/same_size.txt", "versions 1")
    test.backup(offset=1)

    test.write_post("src/same_size.txt", "versions 2")  # Same size!
    test.backup(offset=3)

    if not reuse_hashes:
        assert os.path.exists("dst/same_size.19700101000003.txt")
        assert not os.path.exists("cache/DFB/test_reuse_hashes.checksum.db")
    elif reuse_hashes == "mtime":
        assert os.path.exists("dst/same_size.19700101000003.txt")
        assert os.path.exists("cache/DFB/test_reuse_hashes.checksum.db")
    elif reuse_hashes == "size":
        assert not os.path.exists("dst/same_size.19700101000003.txt")
        assert os.path.exists("cache/DFB/test_reuse_hashes.checksum.db")


def test_missing_hashes():
    test = testutils.Tester(name="missing_hashes")
    test.config["reuse_hashes"] = False
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


if __name__ == "__main__":
    test_main("reference")
    #     test_main("copy")
    #     test_shell()
    #     test_log_upload(True)
    #     test_log_upload(False)
    #     test_dst_compare_and_dst_renames("mtime")
    #     test_dst_compare_and_dst_renames(False)
    #     test_shell_scripts('reference')
    #     test_shell_scripts('copy')
    #     test_restore_error()
    #     for mode in ["size", "mtime", "hash", "hash_mtime"]:
    #         test_false_negs_compare(mode)
    #     test_missing_ref()
    #     test_override()
    #     test_subdirs()
    #     for mode, shell in itertools.product(["link", "copy", "skip"], [True, False]):
    #         test_symlinks(mode, shell)
    #     test_symlinks_trick()
    #     test_keep_going_on_fail()
    #    test_snapshots(True)
    #    test_snapshots(False)
    #     for reuse_hashes in [False, "mtime", "size"]:
    #         test_reuse_hashes_method(reuse_hashes)
    #     test_missing_hashes()
    print("=" * 50)
    print(" All Passed ".center(50, "="))
    print("=" * 50)
