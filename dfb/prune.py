"""
Prune
"""
import datetime
import re
import bisect
import subprocess
import shlex
from operator import itemgetter

_r = repr

from . import log, debug, LOCK
from .utils import bytes2human, shell_header
from .timestamps import timestamp_parser
from .dstdb import DFBDST
from .rclone import rcpathjoin
from .threadmapper import thread_map_unordered as tmap


class Prune:
    def __init__(self, config):
        self.config = config
        self.args = config.cliconfig

        if config.disable_prune and not self.args.dry_run:
            log(
                "Setting --dry-run based on 'disable_prune = True'. "
                "Run with `--override 'disable_prune = False'` to override"
            )
            self.args.dry_run = True

        self.when = when = timestamp_parser(
            self.args.when, epoch=True, now=self.config.now.obj
        )
        self.dstdb = PruneableDFBDST(config)

        log(f"Pruning to {timestamp_parser(self.when,aware=True).isoformat()}")
        self.rpaths = self.dstdb.prune_rpaths(self.when, subdir=self.args.subdir)
        if not self.rpaths:
            log("Nothing to prune")
            return

        self.summary()

        if self.args.dry_run:
            log("DRY-RUN. Exit")
            return
        elif self.args.interactive:
            r = input("Do you want to continue? [Y]/N:")
            if r.lower().startswith("n"):
                return

        rpaths = (r[0] for r in self.rpaths)

        if self.args.shell_script:
            cmd = [config.rclone_exe] + config.rclone_flags + ["delete"]
            out = [shell_header(config, cd=True)]
            for rpath in rpaths:
                out.append(shlex.join(cmd + [rcpathjoin(self.config.dst, rpath)]))

            if self.args.shell_script == "-":
                log.print("\n".join(out), flush=True)
            else:
                with open(self.args.shell_script, "wt") as fp:
                    fp.write("\n".join(out))
                log(f"Shell script written to {_r(self.args.shell_script)}")
            return

        self.errcount = 0

        rc = self.config.rc
        rc.start_rc()

        def _delete(rpath):
            try:
                log(f"Pruning {_r(rpath)}.")
                rc.delete((self.config.dst, rpath))
                return rpath
            except subprocess.CalledProcessError as EE:
                log(f"ERROR: Could not prune {_r(rpath)}.")
                log(f"Error: {EE}")
                with LOCK:
                    self.errcount += 1

        rpaths = tmap(_delete, rpaths, Nt=self.config.concurrency)
        rpaths = filter(bool, rpaths)  # Remove errors
        rpaths = map(self.dstdb.delete_rpath, rpaths)  # on main thread only
        for _ in rpaths:
            pass

        if self.errcount:
            msg = "ERROR: At least one prune delete did not work."
            log(msg)
            raise ValueError(msg)

    def summary(self):
        _p = debug
        if self.args.dry_run or self.args.interactive:
            _p = log

        num, units = bytes2human(sum(r[-1] for r in self.rpaths if r[-1] >= 0))
        s = "s" if len(self.rpaths) != 1 else ""
        log(f"Pruning {len(self.rpaths)} file{s} ({num:0.2f} {units})")

        for rpath, size in self.rpaths:
            num, units = bytes2human(size)
            paren = f"{num:0.2f} {units}" if size >= 0 else "DEL"
            _p(f"    {_r(rpath)} ({paren})")


class PruneableDFBDST(DFBDST):
    def prune_rpaths(self, when, subdir=""):
        # Pruning is more complex than it seems at first because of reference files.
        # We do not want to delete files still being references. We also don't want
        # to delete a delete-marker that "hides" those still-referenced files.
        # The algorithm to do this is described in comments
        #
        # Because of this added complexity, subdir is just a convenience to filter those
        # who do not start with it. The full prune is considered in step 1
        subdir = f"{subdir.removesuffix('/').removeprefix('./')}/".removeprefix("/")

        # Step 0: Group by aname
        groups = self.group_by_apath(select="rpath,apath,timestamp,size")

        # Step 1a: Bisect the group to find the cutoff spot, when <= ts
        #          (<= means bisect_right). icut = iwhen - 1
        # Step 1b: If there is nothing before the cutoff, keep it all (icut = 0)
        # Step 1c: Keep everything to the right of icut. Universal set
        # Step 1d: Save the group to the left of icut. Grouped list
        keep_rpaths = set()
        del_groups = {}
        for name, group in groups:
            iwhen = keyed_bisect_right(group, when, key=itemgetter("timestamp"))  # 1a
            icut = max([iwhen - 1, 0])  # 1a,b
            keep_rpaths.update(row["rpath"] for row in group[icut:])
            del_groups[name] = group[:icut]

            debug(f'(1) {_r(name)} keep {[row["rpath"] for row in group[icut:]]}')
            debug(
                f'(1) {_r(name)} consider for del {[row["rpath"] for row in group[icut:]]}'
            )

        # Step 2: Loop over each group of to-be-deleted files
        # Step 2a: Delete everything that isn't (1) referenced (i.e. in keep_rpaths)
        #          or (2) a delete marker
        # Step 2b: Remove any delete markers iff not the last item
        # Step 2c: If and only if there is only ONE remaining item and it is a delete
        #          marker, delete it. (This makes sure that a delete marker "hides"
        #          a kept file, that is kept because of reference)
        del_rpaths = set()
        for name, group in del_groups.items():
            if subdir and not name.startswith(subdir):
                debug(f"subdir = {_r(subdir)} filter on {_r(name)}")
                continue

            _d = set()  # This isn't needed but makes debug msg easier
            keep_group = []
            for row in group:  # 2a
                if row["rpath"] in keep_rpaths or row["size"] < 0:  # (1)  # (2)
                    keep_group.append(row)  # do not delete yet
                    continue
                _d.add((row["rpath"], row["size"]))
            del_rpaths.update(_d)

            debug(f'(2a) {_r(name)} temp keep {[row["rpath"] for row in keep_group]}')
            debug(f"(2a) {_r(name)} del {_d}")

            if not keep_group:
                continue

            # 2b
            _d = set()
            still_keep = []
            for row in keep_group[:-1]:  # 2b. Do not consider the last row
                if row["size"] < 0:
                    _d.add((row["rpath"], row["size"]))
                else:
                    still_keep.append(row)
            still_keep.append(keep_group[-1])  # Add the last item back
            del_rpaths.update(_d)

            debug(f'(2b) {_r(name)} temp keep {[row["rpath"] for row in still_keep]}')
            debug(f"(2b) {_r(name)} del {_d}")

            # 2c
            if len(still_keep) > 1:
                continue
            row = still_keep[0]
            if row["size"] < 0:
                del_rpaths.add((row["rpath"], row["size"]))

                debug(f'(2c) {_r(name)} del {_r(row["rpath"])}')

        # A note: This can be made even more agressive because this may leave behind an
        #         unneeded delete marker. For the sake of simplicity, I am not going to
        #         worry about that!
        #
        # This section has 100% test coverage! Very good to have with the complexities

        return del_rpaths

    def delete_rpath(self, rpath):
        db = self.db()
        with db:
            db.execute(
                """
                DELETE FROM items 
                WHERE rpath = ?""",
                (rpath,),
            )
        db.commit()
        db.close()
        return rpath


class KeyedListWrapper:
    """
    Create a wrapper for a list with keys. This isn't needed in Python 3.10+
    Based on https://gist.github.com/ericremoreynolds/2d80300dabc70eebc790
    """

    def __init__(self, mylist, key, cache=False):
        self.mylist = mylist
        self.key = key
        if cache:
            self.cache = [self.key(item) for item in mylist]
        else:
            self.cache = []

    def __len__(self):
        return len(self.mylist)

    def __getitem__(self, ix):
        try:
            return self.cache[ix]
        except IndexError:
            return self.key(self.mylist[ix]) if self.key else self.mylist[ix]


def keyed_bisect(a, x, *, key=None, cache=False, **kwargs):
    return bisect.bisect(KeyedListWrapper(a, key, cache=cache), x, **kwargs)


keyed_bisect_right = keyed_bisect


def keyed_bisect_left(a, x, *, key=None, cache=False, **kwargs):
    return bisect.bisect_left(KeyedListWrapper(a, key, cache=cache), x, **kwargs)
