import hashlib
import json
import logging
import os
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from textwrap import dedent
from threading import Barrier, Thread

# 3rd Party
import pytest
import requests
from urllib3.util import connection
from urllib3.util.retry import Retry

if (p := os.path.abspath("../")) not in sys.path:
    sys.path.insert(0, p)


from dfb import rclonerc
from dfb.rclonerc import RC, rcpathjoin, rcpathsplit

TESTDIRS = Path(__file__).parent / "testdirs"


def rmdir(path):
    try:
        shutil.rmtree(path)
    except OSError:
        pass


@pytest.fixture
def http_rc(monkeypatch):
    """Exercise the real HTTP client against a persistent local connection."""
    clients = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):
            clients.append(self.client_address)
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            if self.path.startswith("/disconnect"):
                self.close_connection = True
                return
            error = self.path.startswith("/error")
            body = json.dumps({"error": "test error"} if error else {}).encode()
            self.send_response(500 if error else 200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    rc = RC()
    rc.addr = f"127.0.0.1:{server.server_port}"
    monkeypatch.setattr(rc, "start", lambda: rc)
    try:
        yield rc, clients
    finally:
        rc.stop()
        server.shutdown()
        server.server_close()
        thread.join()


def test_http_connection_reuse_and_cleanup(http_rc, monkeypatch):
    rc, clients = http_rc
    for _ in range(5):
        assert rc.call("rc/noop") == {}
    assert len(set(clients)) == 1

    barrier = Barrier(3)

    def worker(_):
        barrier.wait(timeout=5)
        for _ in range(5):
            rc.call("rc/noop")

    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(worker, range(3)))
    assert len(clients) == 20
    assert len(set(clients)) == 4  # Main thread plus three worker connections.

    closed = []
    sessions = list(rc._sessions)
    for session in sessions:
        close = session.close

        def record_close(session=session, close=close):
            closed.append(session)
            close()

        monkeypatch.setattr(session, "close", record_close)
    rc.stop()
    rc.stop()  # Cleanup is idempotent.
    assert closed == sessions
    assert not rc._sessions
    rc.call("rc/noop")
    assert len(set(clients)) == 5  # Closed sessions aren't reused.


@pytest.mark.parametrize("recover", [True, False])
def test_http_connect_retries(http_rc, monkeypatch, recover):
    rc, clients = http_rc
    connect = connection.create_connection
    attempts = []
    sleeps = []

    def flaky_connect(*args, **kwargs):
        attempts.append(args)
        if not recover or len(attempts) < 4:
            raise OSError(49, "Can't assign requested address")
        return connect(*args, **kwargs)

    def record_backoff(retry, response=None):
        sleeps.append(retry.get_backoff_time())

    monkeypatch.setattr(connection, "create_connection", flaky_connect)
    monkeypatch.setattr(Retry, "sleep", record_backoff)
    if recover:
        assert rc.call("rc/noop") == {}
        assert len(clients) == 1
    else:
        with pytest.raises(requests.ConnectionError):
            rc.call("rc/noop")
        assert not clients
    assert len(attempts) == 4
    assert sleeps == [0, 1, 2]


@pytest.mark.parametrize("endpoint", ["disconnect", "error"])
def test_http_does_not_repeat_submitted_operations(http_rc, endpoint):
    rc, clients = http_rc
    error = (
        requests.ConnectionError if endpoint == "disconnect" else rclonerc.RcloneError
    )
    with pytest.raises(error):
        rc.call(endpoint)
    assert len(clients) == 1


# do all tests in one  since it can then use the same rc
def test_main():
    # if True:
    testpath = TESTDIRS / "rcmain"
    rmdir(testpath)
    testpath.mkdir(exist_ok=True, parents=False)

    config = dedent(f"""\
    [src]
    type = alias
    remote = {str(testpath / 'A')}

    [dst]
    type = alias
    remote = {str(testpath / 'B')}
    """)

    (testpath / "config.cfg").write_text(config)

    rc_log = logging.getLogger("dfb.rclonerc-rc-server")
    rc_log.setLevel(logging.DEBUG)

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
    assert not rc._started
    assert not rc._sessions
    rc.stop()


if __name__ == "__main__":
    test_main()

    print("=" * 50)
    print(" PASS ".center(50, "="))
    print("=" * 50)
