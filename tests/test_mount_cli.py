import argparse
import sys
from pathlib import Path

p = str(Path(__file__).resolve().parents[1])
if p not in sys.path:
    sys.path.insert(0, p)

from dfb.mount.cli import _add_mount_args


def _parse_mount_args(argv):
    parser = argparse.ArgumentParser()
    _add_mount_args(parser)
    return parser.parse_args(argv)


def test_mount_cli_defaults():
    args = _parse_mount_args(["rclone_mnt", "dfb_mnt"])
    assert args.rclone_mount_loc == "rclone_mnt"
    assert args.mount_dest == "dfb_mnt"
    assert args.remove_empty_dirs is False
    assert args.empty_check_fail == "hide"
    assert args.undated_files == "tag"


def test_mount_cli_remove_empty_boolean_optional():
    args = _parse_mount_args(["rclone_mnt", "dfb_mnt", "--remove-empty-dirs"])
    assert args.remove_empty_dirs is True

    args = _parse_mount_args(["rclone_mnt", "dfb_mnt", "--no-remove-empty-dirs"])
    assert args.remove_empty_dirs is False
