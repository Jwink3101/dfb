import os
import sys
from pathlib import Path

p = str(Path(__file__).resolve().parents[1])
if p not in sys.path:
    sys.path.insert(0, p)

from dfb.mount.view import DFBMountView


def _touch(path, text="x"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wt") as fp:
        fp.write(text)


def test_undated_files_modes(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    _touch(root / "dated.19700101000001.txt")
    _touch(root / "plain.txt")

    show_view = DFBMountView(str(root), undated_files="show")
    show_items = set(show_view.listdir("/"))
    assert "/dated.txt" in show_items
    assert "/plain.txt" in show_items

    hide_view = DFBMountView(str(root), undated_files="hide")
    hide_items = set(hide_view.listdir("/"))
    assert "/dated.txt" in hide_items
    assert "/plain.txt" not in hide_items

    tag_view = DFBMountView(str(root), undated_files="tag")
    tag_items = set(tag_view.listdir("/"))
    assert "/dated.txt" in tag_items
    assert "/plain.txt [NODATE]" in tag_items


def test_remove_empty_respects_undated_mode(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    _touch(root / "undated_only" / "plain.txt")

    hide_view = DFBMountView(
        str(root), remove_empty=True, undated_files="hide", empty_check_depth=None
    )
    assert "/undated_only" not in set(hide_view.listdir("/"))

    show_view = DFBMountView(
        str(root), remove_empty=True, undated_files="show", empty_check_depth=None
    )
    assert "/undated_only" in set(show_view.listdir("/"))

    tag_view = DFBMountView(
        str(root), remove_empty=True, undated_files="tag", empty_check_depth=None
    )
    assert "/undated_only" in set(tag_view.listdir("/"))


def test_empty_check_fail_controls_depth_limit_uncertainty(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    _touch(root / "outer" / "inner" / "live.19700101000001.txt")

    hide_view = DFBMountView(
        str(root),
        remove_empty=True,
        empty_check_depth=0,
        empty_check_fail="hide",
    )
    assert "/outer" not in set(hide_view.listdir("/"))

    show_view = DFBMountView(
        str(root),
        remove_empty=True,
        empty_check_depth=0,
        empty_check_fail="show",
    )
    assert "/outer" in set(show_view.listdir("/"))
