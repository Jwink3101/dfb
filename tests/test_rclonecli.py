#!/usr/bin/env python
# -*- coding: utf-8 -*-
import hashlib
import io
import os
import shutil
import subprocess
import sys
from textwrap import dedent

# 3rd Party
import pytest

p = os.path.abspath("../")
if p not in sys.path:
    sys.path.insert(0, p)

from dfb import rclonecli
from dfb.rclonecli import RcloneCLI

# assert os.path.relpath(rclonecli.__file__, ".") == "rclone.py"  # Must be local!


def rmdir(path):
    try:
        shutil.rmtree(path)
    except OSError:
        pass


def write_config(name):
    try:
        os.makedirs(f"testdirs")
    except:
        pass
    with open(f"testdirs/{name}.cfg", "wt") as fp:
        fp.write(
            dedent(
                f"""\
            [myremote]
            type = alias
            remote = testdirs/{name}"""
            )
        )
    return f"testdirs/{name}.cfg"


def test_main():
    # if True:
    rmdir("testdirs/main")
    cfg = write_config("main")
    rclone = RcloneCLI(
        "myremote:",
        universal_flags=["--config", cfg],
        universal_env={"RCLONE_PASSWORD_COMMAND": RcloneCLI.DELENV},
    )

    #####################
    ## Uploads
    #####################

    with open("testdirs/tmpfile.txt", "wt") as fp:
        fp.write("Test File")

    rclone.upload("testdirs/tmpfile.txt")
    assert rclone.read("tmpfile.txt").decode() == "Test File"

    rclone.uploadto("testdirs/tmpfile.txt", "sub/dir/file.txt")
    assert rclone.read("sub/dir/file.txt").decode() == "Test File"

    with open("testdirs/tmpfile2.txt", "wt") as fp:
        fp.write("Test File2")

    rclone.uploadmany(
        [
            "tmpfile.txt",
            "tmpfile2.txt",
        ],
        destdir="many",
        upload_from="testdirs",
    )
    assert rclone.read("many/tmpfile.txt").decode() == "Test File"
    assert rclone.read("many/tmpfile2.txt").decode() == "Test File2"

    # Stream Upload takes four forms: (1) string, (2) bytes, (3) bytes file object
    # (3.5) subprocess.PIPE, (4) iterator/generator. And it needs to be tested
    # with and without fallback mode
    for fallback in [True, False, "auto"]:
        rmdir("testdirs/main/stream")

        rclone.write(b"\x00bytes\x00", "stream/bytes.txt", fallback=fallback)
        rclone.write("strings", "stream/strings.txt", fallback=fallback)
        with open("testdirs/tmpfile.txt", "rb") as fp:
            rclone.write(fp, "stream/file.txt", fallback=fallback)

        proc = subprocess.Popen(["echo", "proc"], stdout=subprocess.PIPE)
        rclone.write(proc.stdout, "stream/proc.txt", fallback=fallback)

        rclone.write(
            (f"line {i}\n".encode() for i in range(10)),
            "stream/generator.txt",
            fallback=fallback,
        )

        files = list(rclone.listremote(subdir="stream"))
        assert {(file["Path"], file["Size"]) for file in files} == {
            ("bytes.txt", 7),
            ("file.txt", 9),
            ("generator.txt", 70),
            ("proc.txt", 5),
            ("strings.txt", 7),
        }

    #####################
    ## Delete
    #####################

    list(rclone.listremote(subdir="tmpfile.txt"))[0]
    rclone.delete("tmpfile.txt", rmdirs=False)
    try:
        list(rclone.listremote(subdir="tmpfile.txt"))[0]
        assert False
    except subprocess.CalledProcessError:
        pass
    assert len(list(rclone.listremote(subdir="many"))) == 2
    rclone.delete("many")
    assert len(list(rclone.listremote(subdir="many"))) == 0

    #####################
    ## Download
    #####################
    rclone.download("stream/file.txt", "testdirs")
    assert open("testdirs/file.txt").read() == "Test File"
    rclone.downloadto("stream/file.txt", "testdirs/afile.txt")
    assert open("testdirs/afile.txt").read() == "Test File"

    #####################
    ## File Object
    #####################

    # Streamupload
    file1 = rclone / "sub" / "1/2" / "file.txt"
    file1.write("uploaded text")
    assert open("testdirs/main/sub/1/2/file.txt").read() == "uploaded text"

    # Upload. Can be local or a remote
    file2 = rclone / "file.txt"
    file2.upload("testdirs/file.txt")
    file2.copy("subdir")
    file2.copyto("subdir/file copy.txt")
    file2.move("moveddir")  # Will *also* change where this is pointed!
    assert file2.remoteitem == "moveddir/file.txt"
    file2.moveto("new/dir/moved.txt")
    assert file2.remoteitem == "new/dir/moved.txt"
    file2.info()

    ##############################
    ## Backend and cached property
    ##############################
    # WARNING: This may break with future rclone versions
    with rclone.capture() as cap:
        assert set(rclone.config_paths) == {"Cache dir", "Config file", "Temp dir"}

        assert rclone.backend_features["Name"] == "local"

        # This should *not* cause another call
        assert rclone.features["About"] == True
        assert rclone.backend_features["Features"]["About"] == True

        assert len(cap.command_history) == 2


def test_fobj():
    rmdir("testdirs/fobj")
    cfg = write_config("fobj")
    rclone = RcloneCLI(
        "myremote:",
        universal_flags=["--config", cfg],
        universal_env={"RCLONE_PASSWORD_COMMAND": RcloneCLI.DELENV},
    )

    rclone.write((os.urandom(1024 * 1024) for _ in range(10)), "random.bin")

    # Test with hashes...
    # Make sure we cross boundaries at odd ways by using prime numvers
    with (
        open("testdirs/fobj/random.bin", "rb") as fp1,
        rclone.open(
            "random.bin",
            "rb",
            buffer_size=1000159,  # 976.718 kb, prime number
        ) as fp2,
    ):
        h1 = hashlib.md5()
        h2 = hashlib.md5()

        while b1 := fp1.read(995611):  # 972.276 kb, prime
            h1.update(b1)
        while b2 := fp2.read(995611):
            h2.update(b2)

        assert h1.digest() == h2.digest()

        fp1.seek(1153)
        fp2.seek(1153)
        h1.update(fp1.read(1001669))
        h2.update(fp2.read(1001669))

        assert h1.digest() == h2.digest()

        fp1.seek(1024 * 1024 * 10)
        fp2.seek(1024 * 1024 * 10)

        h1.update(fp1.read())
        h2.update(fp2.read())

        assert h1.digest() == h2.digest()

    rclone.write("lîné1\nlÚne2", "text.txt")
    with rclone.open("text.txt", mode="rt") as fp:
        assert fp.read() == "lîné1\nlÚne2"

    try:
        rclone.open("text.txt", mode="wt")
        assert False
    except io.UnsupportedOperation:
        pass


def test_streamed_output():
    rmdir("testdirs/streamout")
    cfg = write_config("streamout")
    rclone = RcloneCLI(
        "myremote:",
        universal_flags=["--config", cfg],
        universal_env={"RCLONE_PASSWORD_COMMAND": RcloneCLI.DELENV},
    )
    lines = rclone.write(
        os.urandom(10),
        "small.bin",
        flags=[
            "--bwlimit",
            "5b",
            "--stats",
            "0.1",
            "--stats-one-line",
            "-v",
        ],
        callopts=dict(stream=True),
    )
    for st, line in lines:
        print(st, line.decode(), end="", flush=True)


def test_context_managers():
    rmdir("testdirs/context")
    cfg = write_config("context")
    rclone = RcloneCLI(
        "myremote:",
        universal_flags=["--config", cfg],
        universal_env={
            "RCLONE_PASSWORD_COMMAND": RcloneCLI.DELENV,
            "TEST_ENV": "Testi ng",
        },
    )

    with rclone.capture() as cap:
        rclone.write("test data", "test.txt")
    assert cap.command_history[0] == [
        "rclone",
        "--config",
        "testdirs/context.cfg",
        "rcat",
        "myremote:test.txt",
        "--no-traverse",
        "--no-check-dest",
        "--size",
        "9",
    ]

    with rclone.capture(save_results=True) as cap:
        print(rclone.read("test.txt").decode())
    assert cap.command_history[0] == [
        "rclone",
        "--config",
        "testdirs/context.cfg",
        "cat",
        "myremote:test.txt",
    ]
    assert cap.command_results[0][0] == b"test data"

    # Should do nothing
    with rclone.capture(execute=False) as cap:
        assert rclone.read("test.txt") == None

    # Make sure this works
    with rclone.capture(execute=True) as cap:
        list(rclone.listremote())

    # This should cause a downstream failure
    try:
        with rclone.capture(execute=False) as cap:
            list(rclone.listremote())
        assert False
    except TypeError:
        assert True

    # test the not always
    with open("testdirs/tmpfile.txt", "wt") as fp:
        fp.write("Test File")

    with rclone.capture(save_results=True, save_stdin=True) as cap:
        not_there = "tmpfile.txt: Need to transfer - File not found at Destination"
        unchanged = "tmpfile.txt: Unchanged skipping"
        uncond = "tmpfile.txt: Transferring unconditionally as --ignore-times is in use"
        # First one
        rclone.upload("testdirs/tmpfile.txt", flags="-vv")
        assert not_there in cap.command_results[-1][1].decode()
        assert unchanged not in cap.command_results[-1][1].decode()
        assert uncond not in cap.command_results[-1][1].decode()

        assert "--no-check-dest" in cap.command_history[-1]
        assert "--ignore-times" not in cap.command_history[-1]

        # Still not "there" because it can't check
        rclone.upload("testdirs/tmpfile.txt", flags="-vv")
        assert not_there in cap.command_results[-1][1].decode()
        assert unchanged not in cap.command_results[-1][1].decode()
        assert uncond not in cap.command_results[-1][1].decode()

        assert "--no-check-dest" in cap.command_history[-1]
        assert "--ignore-times" not in cap.command_history[-1]

        # Now it shouldn't upload
        with rclone.not_always:
            rclone.upload("testdirs/tmpfile.txt", flags="-vv")
        assert not_there not in cap.command_results[-1][1].decode()
        assert unchanged in cap.command_results[-1][1].decode()
        assert uncond not in cap.command_results[-1][1].decode()

        assert "--no-check-dest" not in cap.command_history[-1]
        assert "--ignore-times" not in cap.command_history[-1]

        # Test with uploads
        rclone.write("some data", "testdirs/here.txt")  # Should be fine
        rclone.write(b"\xb9\xd5\x85\x85j\xa6-7\xb4\xe3", "testdirs/here.txt")
        rclone.write(io.BytesIO(os.urandom(10)), "testdirs/here.txt")

        shell = cap.shell_script()
        assert "unset RCLONE_PASSWORD_COMMAND" in shell
        assert "echo" in shell
        assert "Could not understand stdin" in shell
        assert "stdin specified and not bytes" in shell

    # Repeat the above with `ignore_times`
    rclone = RcloneCLI(
        "myremote:",
        universal_flags=["--config", cfg],
        universal_env={"RCLONE_PASSWORD_COMMAND": RcloneCLI.DELENV},
        no_check_dest=False,
    )
    rclone.delete("tmpfile.txt")

    with rclone.capture(save_results=True) as cap:
        # First one
        rclone.upload("testdirs/tmpfile.txt", flags="-vv")
        assert not_there in cap.command_results[-1][1].decode()
        assert unchanged not in cap.command_results[-1][1].decode()
        assert uncond not in cap.command_results[-1][1].decode()

        assert "--no-check-dest" not in cap.command_history[-1]
        assert "--ignore-times" in cap.command_history[-1]

        # Try again. DIFFERENT from above
        rclone.upload("testdirs/tmpfile.txt", flags="-vv")
        assert not_there not in cap.command_results[-1][1].decode()
        assert unchanged not in cap.command_results[-1][1].decode()
        assert uncond in cap.command_results[-1][1].decode()

        assert "--no-check-dest" not in cap.command_history[-1]
        assert "--ignore-times" in cap.command_history[-1]

        # Now it shouldn't upload
        with rclone.not_always:
            rclone.upload("testdirs/tmpfile.txt", flags="-vv")
        assert not_there not in cap.command_results[-1][1].decode()
        assert unchanged in cap.command_results[-1][1].decode()
        assert uncond not in cap.command_results[-1][1].decode()

        assert "--no-check-dest" not in cap.command_history[-1]
        assert "--ignore-times" not in cap.command_history[-1]


if __name__ == "__main__":
    test_main()
    test_fobj()
    test_streamed_output()
    test_context_managers()

    print("-" * 50)
    print("-- PASSED --")
    pass
