#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
from pathlib import Path
import gzip as gz
import subprocess
import json
from textwrap import dedent

p = os.path.abspath("../")
if p not in sys.path:
    sys.path.insert(0, p)

# Local
import testutils

# testing
import pytest


def test_basic_cases():
    test = testutils.Tester(name="basic_cases")

    """
       init  del3  del5  del7   dm5   dm7   mix 
     1   *     *     *     *     *     *     *  
               │     │     │     │     │     │  
               │     │     │     │     │     │  
     3         D     │     │     *     *     D  
                     │     │     │     │        
                     │     │     │     │        
     5               D     │     D     *     *  
                           │           │     │  
                           │           │     │  
     7                     D           D     D   
     """

    # + tags=[]
    test.config["renames"] = False
    test.config["reuse_hashes"] = False
    test.config["compare"] = "hash"
    test.write_config()
    vq = ["-q"]

    # + tags=[]
    test.write_pre("src/init.txt", "init")
    test.write_pre("src/del3.txt", "del3")
    test.write_pre("src/del5.txt", "del5")
    test.write_pre("src/del7.txt", "del7")
    test.write_pre("src/dm5.txt", "dm5")
    test.write_pre("src/dm7.txt", "dm7")
    test.write_pre("src/mix.txt", "mix")
    test.backup(*vq, offset=1)

    os.unlink("src/del3.txt")
    test.write_pre("src/dm5.txt", "dm5.3")
    test.write_pre("src/dm7.txt", "dm7.3")
    os.unlink("src/mix.txt")
    test.backup(*vq, offset=3)

    os.unlink("src/del5.txt")
    os.unlink("src/dm5.txt")
    test.write_pre("src/dm7.txt", "dm7.5")
    test.write_pre("src/mix.txt", "mix2")
    test.backup(*vq, offset=5)

    os.unlink("src/del7.txt")
    os.unlink("src/dm7.txt")
    os.unlink("src/mix.txt")
    test.backup(*vq, offset=7)

    # + tags=[]
    prune = test.call("prune", "--dry-run", "u2", *vq)
    assert set(prune.rpaths) == set()

    # + tags=[]
    prune = test.call("prune", "--dry-run", "u4", *vq)
    assert set(prune.rpaths) == {
        ("del3.19700101000001.txt", 4),
        ("dm5.19700101000001.txt", 3),
        ("dm7.19700101000001.txt", 3),
        ("mix.19700101000001.txt", 3),
    }
    # -

    prune = test.call("prune", "--dry-run", "u6", *vq)
    assert set(prune.rpaths) == {
        ("del3.19700101000001.txt", 4),
        # Theoretically could have del3.19700101000003D.txt but this is missed as per the note
        ("del5.19700101000001.txt", 4),
        ("dm5.19700101000001.txt", 3),
        ("dm5.19700101000003.txt", 5),
        ("dm7.19700101000001.txt", 3),
        ("dm7.19700101000003.txt", 5),
        ("mix.19700101000001.txt", 3),
        ("mix.19700101000003D.txt", -1),  # Example of 2C delete
    }

    prune = test.call("prune", "--dry-run", "u8", *vq)
    assert set(prune.rpaths) == {
        ("del3.19700101000001.txt", 4),
        # Theoretically could have del3.19700101000003D.txt but this is missed as per the note
        ("del5.19700101000001.txt", 4),
        ("dm5.19700101000001.txt", 3),
        ("dm5.19700101000003.txt", 5),
        ("dm7.19700101000001.txt", 3),
        ("dm7.19700101000003.txt", 5),
        ("mix.19700101000001.txt", 3),
        ("mix.19700101000003D.txt", -1),  # Example of 2C delete
    }.union(  # These are now removable
        {
            ("del7.19700101000001.txt", 4),
            ("dm7.19700101000005.txt", 5),
            ("mix.19700101000005.txt", 4),
        }
    )

    # Now do it for real. They should result is a different group but they should build on the differences

    prune = test.call("prune", "u2", *vq)

    # Should be the same
    prune = test.call("prune", "u4", *vq)

    prune = test.call("prune", "u6", *vq)
    assert set(prune.rpaths) == (
        {  # above's u6
            ("del3.19700101000001.txt", 4),
            ("del5.19700101000001.txt", 4),
            ("dm5.19700101000001.txt", 3),
            ("dm5.19700101000003.txt", 5),
            ("dm7.19700101000001.txt", 3),
            ("dm7.19700101000003.txt", 5),
            ("mix.19700101000001.txt", 3),
            ("mix.19700101000003D.txt", -1),  # Example of 2C delete
        }
        - {
            ("del3.19700101000001.txt", 4),
            ("dm5.19700101000001.txt", 3),
            ("dm7.19700101000001.txt", 3),
            ("mix.19700101000001.txt", 3),
        }
    )

    prune = test.call("prune", "--dry-run", "u8", *vq)
    assert set(prune.rpaths) == {
        ("del7.19700101000001.txt", 4),
        ("dm7.19700101000005.txt", 5),
        ("mix.19700101000005.txt", 4),
    }


def test_moves():
    """
        f0─┐  f1    f2   f3
     1   * └─┐
         │   └─┐
         │     │
     3   D     R


     5   * ───┐
         │    └───┐
         │        └───
     7   D           R
                     │
                     │
     9   *           *    *
         │           │
         │           │
    11   D           *
    """
    test = testutils.Tester(name="moves")

    # + tags=[]
    test.config["renames"] = "hash"
    test.config["reuse_hashes"] = False
    test.config["compare"] = "hash"
    test.write_config()
    vq = ["-q"]

    # + tags=[]
    test.write_pre("src/f0.txt", "f0")
    test.backup(*vq, offset=1)

    test.move("src/f0.txt", "src/f1.txt")
    test.backup(*vq, offset=3)

    test.write_pre("src/f0.txt", "f0-2")
    test.backup(*vq, offset=5)

    test.move("src/f0.txt", "src/f2.txt")
    test.backup(*vq, offset=7)

    test.write_pre("src/f0.txt", "f0-3")
    test.write_pre("src/f2.txt", "mod")
    test.write_pre("src/f3.txt", "f3")
    test.backup(*vq, offset=9)

    os.unlink("src/f0.txt")
    test.write_pre("src/f2.txt", "mods")
    test.backup(*vq, offset=11)

    # + tags=[]
    prune = test.call("prune", "--dry-run", "u2", *vq)
    assert set(prune.rpaths) == set()

    # + tags=[]
    prune = test.call("prune", "--dry-run", "u4", *vq)
    assert set(prune.rpaths) == set()  # All are references

    # + tags=[]
    prune = test.call("prune", "--dry-run", "u6", *vq)
    assert set(prune.rpaths) == set()  # All are STILL references

    # + tags=[]
    prune = test.call("prune", "--dry-run", "u8", *vq)
    # No longer need  the D to block the ref since there is a new one
    assert set(prune.rpaths) == {("f0.19700101000003D.txt", -1)}

    # + tags=[]
    prune = test.call("prune", "--dry-run", "u10", *vq)
    assert set(prune.rpaths) == {
        ("f0.19700101000003D.txt", -1),  # From above
        (
            "f0.19700101000005.txt",
            4,
        ),  # Now  blocked by 7D and no longer referecned by f2.7!
    }

    # + tags=[]
    prune = test.call("prune", "--dry-run", "u12", *vq)
    assert set(prune.rpaths) == {
        ("f0.19700101000003D.txt", -1),  # From above
        ("f0.19700101000005.txt", 4),  # Now  blocked by 7D
        ("f0.19700101000009.txt", 4),
        ("f2.19700101000009.txt", 3),
    }
    # -

    # Useful to help visualize
    test.call("versions", "f0.txt", "--ref-count", "--real-path")
    test.call("versions", "f1.txt", "--ref-count", "--real-path")
    test.call("versions", "f2.txt", "--ref-count", "--real-path")
    test.call("versions", "f3.txt", "--ref-count", "--real-path")


def test_modes():
    test = testutils.Tester(name="modes")

    test.write_config()

    # +
    test.write_pre("src/file.txt", "1")
    test.backup("-q", offset=1)

    test.write_pre("src/file.txt", "1.3")
    test.backup("-q", offset=3)

    test.write_pre("src/file.txt", "1.3.5")
    test.backup("-q", offset=5)

    test.write_pre("src/file.txt", "1.3.7")
    test.backup("-q", offset=7)

    # +
    # **MANUAL** -- It works
    # test.call('prune','u6','-i')
    # -

    prune = test.call("prune", "u6", "--shell-script", "-")
    print(prune.rpaths)

    prune = test.call("prune", "u6", "--shell-script", "prune.sh")
    print(prune.rpaths)
    with open("prune.sh") as fp:
        print(fp.read())


def test_subdir():
    test = testutils.Tester(name="prune_subdir")
    test.write_config()

    vq = ["-q"]

    test.write_pre("src/nothing.txt", "do nothing")
    test.write_pre("src/mod.txt", "will mod .")
    test.write_pre("src/sub1/mod_sub.txt", "will mod in sub .")
    test.write_pre("src/sub2/move_at_5.txt", "will move")
    test.backup(offset=1)

    test.write_pre("src/mod.txt", "will mod ..")
    test.write_pre("src/sub1/mod_sub.txt", "will mod in sub ..")
    test.backup(offset=3)

    test.write_pre("src/mod.txt", "will mod ...")
    test.write_pre("src/sub1/mod_sub.txt", "will mod in sub ...")
    test.move("src/sub2/move_at_5.txt", "src/new/NEW.txt")
    test.backup(offset=5)

    test.write_post("src/new/NEW.txt", "neww")
    test.backup(offset=7)

    # Test it
    assert set(test.call("prune", "--dry-run", "u4", *vq).rpaths) == {
        ("mod.19700101000001.txt", 10),
        ("sub1/mod_sub.19700101000001.txt", 17),
    }
    assert set(test.call("prune", "--dry-run", "u4", "--subdir", "sub1").rpaths) == {
        ("sub1/mod_sub.19700101000001.txt", 17)
    }
    assert (
        set(test.call("prune", "--dry-run", "u6", "--subdir", "sub2", *vq).rpaths)
        == set()
    )

    assert set(
        test.call("prune", "--dry-run", "u8", "--subdir", "sub2", *vq).rpaths
    ) == {("sub2/move_at_5.19700101000001.txt", 9)}
    assert set(test.call("prune", "--dry-run", "u8", *vq).rpaths) == {
        ("mod.19700101000003.txt", 11),
        ("sub1/mod_sub.19700101000001.txt", 17),
        ("mod.19700101000001.txt", 10),
        ("sub1/mod_sub.19700101000003.txt", 18),
        ("sub2/move_at_5.19700101000001.txt", 9),
    }


def test_disable():
    test = testutils.Tester(name="prune_dry")
    test.config["disable_prune"] = True
    test.write_config()

    test.write_pre("src/file.txt", "file")
    test.write_pre("src/to_del.txt", "del")
    test.backup(offset=1)

    test.write_post("src/file.txt", "file")
    os.unlink("src/to_del.txt")
    test.backup(offset=3)

    prune = test.call("prune", "--dry-run", "u4")
    assert set(prune.rpaths) == {
        ("to_del.19700101000001.txt", 3),
        ("file.19700101000001.txt", 4),
    }
    assert os.path.exists("dst/to_del.19700101000001.txt")
    assert os.path.exists("dst/file.19700101000001.txt")

    prune = test.call("prune", "u4")
    assert set(prune.rpaths) == {
        ("to_del.19700101000001.txt", 3),
        ("file.19700101000001.txt", 4),
    }
    assert os.path.exists("dst/to_del.19700101000001.txt")
    assert os.path.exists("dst/file.19700101000001.txt")

    prune = test.call("prune", "u4", "--override", "disable_prune = False")
    assert set(prune.rpaths) == {
        ("to_del.19700101000001.txt", 3),
        ("file.19700101000001.txt", 4),
    }
    assert not os.path.exists("dst/to_del.19700101000001.txt")
    assert not os.path.exists("dst/file.19700101000001.txt")


def test_basic_versions():
    test = testutils.Tester(name="prune_versions_basic")
    test.write_config()

    test.write_pre("src/mod_all.txt", "1")
    test.write_pre("src/nomod.txt", "do not touch")
    test.write_pre("src/del.txt", "del")
    test.write_pre("src/mv.txt", "mv")
    test.backup(offset=1)

    test.write_pre("src/mod_all.txt", "13")
    os.unlink("src/del.txt")
    test.move("src/mv.txt", "src/moved.txt")
    test.backup(offset=3)

    test.write_pre("src/mod_all.txt", "135")
    test.backup(offset=5)

    test.write_pre("src/mod_all.txt", "1357")
    test.backup(offset=7)

    assert set(test.call("prune", "now", "-n").rpaths) == {
        ("del.19700101000001.txt", 3),
        ("mod_all.19700101000001.txt", 1),
        ("mod_all.19700101000003.txt", 2),
        ("mod_all.19700101000005.txt", 3),
    }

    assert set(test.call("prune", "now", "-N", "1", "-n").rpaths) == {
        ("mod_all.19700101000001.txt", 1),
        ("mod_all.19700101000003.txt", 2),
    }

    assert set(test.call("prune", "now", "-N", "2", "-n").rpaths) == {
        ("mod_all.19700101000001.txt", 1),
    }

    # Forward in time. Remove the
    assert set(test.call("prune", "u-1", "-N", "-2", "-n").rpaths) == {
        ("del.19700101000001.txt", 3),
        ("mod_all.19700101000001.txt", 1),
    }

    # Make sure you can't go beyond the latest
    assert set(test.call("prune", "now", "-N", "-200", "-n").rpaths) == {
        ("del.19700101000001.txt", 3),
        ("mod_all.19700101000001.txt", 1),
        ("mod_all.19700101000003.txt", 2),
        ("mod_all.19700101000005.txt", 3),
    }
    assert set(test.call("prune", "u0", "-N", "-200", "-n").rpaths) == {
        ("del.19700101000001.txt", 3),
        ("mod_all.19700101000001.txt", 1),
        ("mod_all.19700101000003.txt", 2),
        ("mod_all.19700101000005.txt", 3),
    }


if __name__ == "__main__":
    #     test_basic_cases()
    #     test_moves()
    #     test_modes()
    #     test_subdir()
    #     test_disable()
    test_basic_versions()

    print("=" * 50)
    print(" All Passed ".center(50, "="))
    print("=" * 50)
