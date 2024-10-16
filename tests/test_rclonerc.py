import os, sys, shutil
from pathlib import Path
from textwrap import dedent
import hashlib
import logging


# 3rd Party
import pytest

if (p := os.path.abspath("../")) not in sys.path:
    sys.path.insert(0, p)


from dfb import rclonerc
from dfb.rclonerc import RC, rcpathjoin, rcpathsplit


def rmdir(path):
    try:
        shutil.rmtree(path)
    except OSError:
        pass


# do all tests in one  since it can then use the same rc
def test_main():
    # if True:
    testpath = Path("testdirs/rcmain")
    rmdir(testpath)
    testpath.mkdir(exist_ok=True, parents=False)

    config = dedent(
        f"""\
    [src]
    type = alias
    remote = {str(testpath / 'A')}

    [dst]
    type = alias
    remote = {str(testpath / 'B')}
    """
    )

    (testpath / "config.cfg").write_text(config)

    rc_log = logging.getLogger("dfb.rclonerc-rc-server")
    rc_log.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler(stream=sys.stderr)
    rc_log.addHandler(stream_handler)

    rc = RC(
        serve_flags=["--config", str(testpath / "config.cfg"), "-vv"],
        rclone_env={"RCLONE_PASSWORD_COMMAND": RC.DELENV},
    )

    # test config settings
    # Test the config setting in flags
    assert set(rc.call("config/listremotes").get("remotes", [])) == {"src", "dst"}

    # Write and read some files
    rc.write("src:file1.txt", b"file1")
    rc.write("src:sub/file2.txt", b"file two")

    assert rc.read("src:file1.txt").strip() == b"file1"
    assert rc.read("src:sub/file2.txt").strip() == b"file two"

    # copy/move/delete
    rc.copyfile(src="src:file1.txt", dst=("dst:subb", "file1.txt"))
    rc.movefile(
        src=("src:", "sub/file2.txt"),
        dst="dst:file2.txt",
        _config={
            "NoCheckDest": True,
            "metadata": True,
        },
    )
    rc.delete("src:file1.txt")

    # verify
    assert not rc.list("src:", only="files")  # should be empty from the above

    # Test some of the listing. Verification comes later
    rc.list("dst:", only="dirs")
    rc.list("dst:", only="files")
    rc.list("dst:", only="files", epoch_time=True)

    assert {f["Path"] for f in rc.list("dst:", only="files")} == {
        "file2.txt",
        "subb/file1.txt",
    }

    assert {f["Path"] for f in rc.list("dst:", only="files", filters="- *2.txt")} == {
        "subb/file1.txt"
    }

    ## Test paths

    test_cases = [
        ("single-file.ext", ("./", "single-file.ext")),
        ("local/file.ext", ("local", "file.ext")),
        ("remote:file.ext", ("remote:", "file.ext")),
        ("remote:sub/file.ext", ("remote:", "sub/file.ext")),
        ("remote:/sub/file.ext", ("remote:", "/sub/file.ext")),
        (":http:sub/file.ext", (":http:", "sub/file.ext")),
        (
            ":http,url='https://example.com':path/to/dir",
            (":http,url='https://example.com':", "path/to/dir"),
        ),
        (
            ":http,url='https://example.com':path/t'o/dir/with'quote",
            (":http,url='https://example.com':", "path/t'o/dir/with'quote"),
        ),
    ]

    for full, gold in test_cases:
        split = rcpathsplit(full)
        assert split == gold
        assert rcpathjoin(*split).removeprefix("./") == full

    assert rcpathjoin("a", "b") == "a/b"
    assert rcpathjoin("a:", "b") == "a:b"
    assert rcpathjoin("a:", "/b") == "a:/b"  # Note that these are unlike os.path.join
    assert rcpathjoin("a", "/b") == "a/b"

    ## listing and stat and agreement on them
    # This is the same as stat on 'dst:file2.txt'
    _stat_from_list = lambda **p: [
        a for a in rc.list("dst:", only="files", **p) if a["Path"] == "file2.txt"
    ][0]

    assert _stat_from_list() == rc.stat("dst:file2.txt")

    assert _stat_from_list(hashes=True) == (
        stat := rc.stat("dst:file2.txt", hashes=True)
    )
    assert "md5" in stat["Hashes"]

    assert _stat_from_list(
        hashes=True, hashtypes=("crc32", "sha1"), epoch_time=True
    ) == (
        stat := rc.stat(
            "dst:file2.txt", hashes=True, hashtypes=("crc32", "sha1"), epoch_time=True
        )
    )
    assert set(stat["Hashes"]) == {"crc32", "sha1"}
    assert isinstance(stat["ModTime"], (float, int))

    ## noops

    rc.call_async_and_wait("rc/noop")
    rc.call("rc/noop")
    rc.call("rc/noopauth")

    try:
        rc.call("rc/error")
        assert False
    except rclonerc.RcloneError:
        pass

    ## File Objects. Test by comparing with a regular one
    assert (
        rc.read("dst:file2.txt")
        == rc.open("dst:file2.txt").read()
        == rc.open("dst:file2.txt", "rt").read().encode()  # Text mode
        == (testpath / "B" / "file2.txt").read_bytes()
    )

    testfile = testpath / "A" / "random.bin"
    with testfile.open("wb") as fp:
        mb1 = os.urandom(1024 * 1024)
        for _ in range(20):
            fp.write(mb1)

    h0 = hashlib.sha1()
    h1 = hashlib.sha1()

    with (
        testfile.open("rb") as fp0,
        rc.open("src:random.bin", buffer_size=6 * 1024 * 1024) as fp1,
    ):
        h0.update(fp0.read(5013))
        h1.update(fp1.read(5013))

        h0.update(fp0.read(1024 * 1024 * 7))
        h1.update(fp1.read(1024 * 1024 * 7))

        fp0.seek(0)
        fp1.seek(0)

        h0.update(fp0.read(100))
        h1.update(fp1.read(100))

        fp0.seek(-10, 1)
        fp1.seek(-10, 1)

        h0.update(fp0.read(100))
        h1.update(fp1.read(100))

        fp0.seek(-105, 2)
        fp1.seek(-105, 2)

        h0.update(fp0.read(100))
        h1.update(fp1.read(100))

        h0.update(fp0.read())
        h1.update(fp1.read())

        fp0.seek(0)
        fp1.seek(0)

        h0.update(fp0.read())
        h1.update(fp1.read())

        assert rcpathsplit(fp1) == ("src:", "random.bin")

        # print(f"{h0.hexdigest() = } == {h1.hexdigest() = }")
        assert h0.digest() == h1.digest()

    rc.check()
    rc.start(check=True)
    rc.stop()


if __name__ == "__main__":
    test_main()

    print("=" * 50)
    print(" PASS ".center(50, "="))
    print("=" * 50)
