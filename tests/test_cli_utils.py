import os
import sys
import json

p = os.path.abspath("../")
if p not in sys.path:
    sys.path.insert(0, p)

from dfb.cli import cli

# Local
import testutils
from testutils import Capture

# testing
import pytest


def test_apath2rpath_cli():
    files = ["name1.ext", "path/to/name2.txt.gz"]
    with Capture() as cap:
        cli(["utils", "apath2rpath", *files, "--date", "20120304T05:06:07Z"])
    assert (
        cap.out.strip()
        == "name1.20120304050607.ext\npath/to/name2.20120304050607.txt.gz"
    )

    with Capture() as cap:
        cli(["utils", "apath2rpath", *files, "--date", "20120304T05:06:07Z", "-0"])
    assert (
        cap.out_bytes.strip()
        == b"name1.20120304050607.ext\x00path/to/name2.20120304050607.txt.gz"
    )


def test_rpath2apath_cli():
    file = "name1.20180811094630.ext"
    with Capture() as cap:
        cli(["utils", "rpath2apath", "name1.20180811094630.ext"])
    assert {
        "apath": "name1.ext",
        "timestamp": "2018-08-11T09:46:30+00:00",
        "flag": "",
    } == json.loads(cap.out)

    with Capture() as cap:
        cli(["utils", "rpath2apath", "name1.20180811094630D.ext"])
    assert {
        "apath": "name1.ext",
        "timestamp": "2018-08-11T09:46:30+00:00",
        "flag": "D",
    } == json.loads(cap.out)

    with Capture() as cap:
        cli(["utils", "rpath2apath", "name1.20180811094630R.ext"])
    assert {
        "apath": "name1.ext",
        "timestamp": "2018-08-11T09:46:30+00:00",
        "flag": "R",
    } == json.loads(cap.out)

    with Capture() as cap:
        cli(
            [
                "utils",
                "rpath2apath",
                "first.20180811094630.ext",
                "second.20220625232247",
            ]
        )

    assert [
        {"apath": "first.ext", "timestamp": "2018-08-11T09:46:30+00:00", "flag": ""},
        {"apath": "second", "timestamp": "2022-06-25T23:22:47+00:00", "flag": ""},
    ] == [json.loads(line) for line in cap.out.splitlines()]


if __name__ == "__main__":
    test_apath2rpath_cli()
    test_rpath2apath_cli()

    print("=" * 50)
    print(" All Passed ".center(50, "="))
    print("=" * 50)
