"""
Prune
"""

import bisect
import datetime
import gzip as gz
import json
import logging
import re
import shlex
import subprocess
import sys
from operator import itemgetter

from . import LOCK
from .dstdb import DFBDST
from .rclonerc import rcpathjoin
from .threadmapper import thread_map_unordered as tmap
from .timestamps import timestamp_parser
from .utils import human_readable_bytes, smart_open

logger = logging.getLogger(__name__)


class Prune:
    def __init__(self, config):
        self.config = config
        self.args = config.cliconfig

        if config.disable_prune and not (self.args.dry_run or self.args.dump):
            logger.info("Setting --dry-run based on 'disable_prune = True' ")
            logger.info("Run with `--override 'disable_prune = False'` to override")
            self.args.dry_run = True

        self.dstdb = PruneableDFBDST(config)

    def bydate(self):
        config = self.config
        cliconfig = self.args

        self.when = when = timestamp_parser(
            self.args.when,
            epoch=True,
            now=self.config.now.obj,
        )

        msg = f"Pruning to {timestamp_parser(self.when,aware=True).isoformat()} "
        if self.args.N > 0:
            msg += f"and keeping {self.args.N} additional older version{'s' if self.args.N>1 else ''}"
        if self.args.N < 0:
            msg += f"plus removing {-self.args.N} additional newer version{'s' if self.args.N<-1 else ''}"
        logger.info(msg.strip() + ".")

        self.rpaths = self.dstdb.prune_rpaths(
            self.when,
            self.args.N,
            subdir=self.args.subdir,
        )  # (rpath,size) pairs
        self.rpaths = sorted(self.rpaths)
        if not self.rpaths:
            logger.info("Nothing to prune")
            return

        self.apply()

    def byrpaths(self):
        config = self.config
        cliconfig = self.args

        db = self.dstdb.db()

        err = False
        rpaths = self.rpaths = set()
        for rpath in cliconfig.rpath:
            res = db.execute(
                """
                SELECT rpath,size,ref_rpath,isref
                FROM items
                WHERE 
                    rpath = ?
                """,
                (rpath,),
            ).fetchall()

            if not res:
                logger.warning(f"No matches for {rpath!r}")
                continue

            reg, ref = [], []
            for row in res:
                {True: ref, False: reg}[bool(row["isref"])].append(row)

            if len(reg) > 1:
                logger.debug(f"Multiple non-references to {rpath = }")

            msg = f"{rpath!r} has {len(res)} entr{'ies' if len(res)>1 else 'y'}. "
            msg += "{len(reg)} regular and {len(ref)} references"
            logger.debug(msg)

            for row in reg:
                rpaths.add((row["rpath"], row["size"]))
                logger.debug(" delete {row['rpath']!r}")

            for row in ref:
                msg = f"Deleteing {rpath!r} will break {row['ref_rpath']!r}."
                if cliconfig.error_if_referenced:
                    err = True
                    logger.error(msg + " Will not continue")
                else:
                    # Add them. Not really an "rpath" but will have the same effect of
                    # deleting the file.
                    rpaths.add((row["ref_rpath"], 0))
                    logger.debug(" delete {row['ref_rpath']!r}")
        if err:
            msg = "References will break. Use '--no-error-if-referenced' flag to force."
            raise BrokenReferenceError(msg)

        self.apply()

    def apply(self):
        self.summary()
        config = self.config
        cliconfig = self.args

        rpaths = (r[0] for r in self.rpaths)
        rpaths_size = dict(self.rpaths)

        if file := cliconfig.dump:
            logger.info(f"Dumping jsonl then exiting")
            try:
                fp = smart_open(file, "wt") if file != "-" else sys.stdout
                for rpath in rpaths:
                    item = {
                        "_V": 1,
                        "_action": "prune",
                        "rpath": rpath,
                        "size": rpaths_size.get(rpath, None),
                    }
                    print(
                        json.dumps(item, ensure_ascii=False, separators=(",", ":")),
                        file=fp,
                        flush=True,
                    )
            finally:
                if file != "-":
                    fp.close()
                    logger.info(f"Written to {file!r}")
            return

        if self.args.dry_run:
            logger.info("DRY-RUN. Exit")
            return
        elif self.args.interactive:
            r = input("Do you want to continue? [Y]/N:")
            if r.lower().startswith("n"):
                return

        self.errcount = 0

        rc = self.config.rc
        rc.start()

        def _delete(rpath):
            try:
                logger.info(f"Pruning {rpath!r}.")
                rc.delete((self.config.dst, rpath))
            except Exception as EE:
                logger.error(
                    f"Could not prune {rpath!r}. " f"Will assume it is deleted. {EE}"
                )
                with LOCK:
                    self.errcount += 1
            return rpath

        rpaths = tmap(_delete, rpaths, Nt=self.config.concurrency)
        # Originally we didn't remove the file if there was an error but it's changed
        # now. First of all, the most likely error is that the file is already missing
        # so this updates the DB. If not, the worst that happens in that it remains and
        # goes unnoticed until a refresh. Future dfb versions may be smarter about what
        # types of errors it handles.
        # rpaths = filter(bool, rpaths)  # Remove errors
        rpaths = map(self.dstdb.delete_rpath, rpaths)  # on main thread only
        for _ in rpaths:
            pass

        self.dstdb.push_snapshots()

    def summary(self):
        _p = logger.debug
        if self.args.dry_run or self.args.interactive:
            _p = logger.info

        num, units = human_readable_bytes(sum(r[-1] for r in self.rpaths if r[-1] >= 0))
        s = "s" if len(self.rpaths) != 1 else ""
        logger.info(f"Pruning {len(self.rpaths)} file{s} ({num:0.2f} {units})")

        for rpath, size in self.rpaths:
            num, units = human_readable_bytes(size)
            paren = f"{num:0.2f} {units}" if size >= 0 else "DEL"
            _p(f"    {rpath!r} ({paren})")


class PruneableDFBDST(DFBDST):
    def prune_rpaths(self, when, keep, subdir=""):
        # Pruning is more complex than it seems at first because of reference files.
        # We do not want to delete files still being referenced. We also don't want
        # to delete a delete-marker that "hides" those still-referenced files.
        # The algorithm to do this is described in comments
        #
        # When subdir is selected, we can consider files that are in the subdir OR
        # point to it (for references). Note, previous behavior was WRONG and would
        # deleted referent apaths when a reference was outside subdir. This is fixed
        # (and tested)
        subdir = f"{subdir.removesuffix('/').removeprefix('./')}/".removeprefix("/")

        # Step 0: Group by aname. These are sorted by timestamp. Only look within the
        #         subdir but will later add things that point inside.
        groups = self.group_by_apath(
            select="rpath,apath,timestamp,size,ref_rpath",
            conditions=("WHERE apath LIKE ?", [subdir + "%"]),
        )

        ## Regular prune -- This would be it if not for references
        # Step 1a: Bisect the group to find the cutoff spot, when <= ts
        #          (<= means bisect_right). Then do icut = iwhen - 1
        #          to account for keeping the element older.
        # Step 1b: Shift by "keep" to keep specified versions
        # Step 1c: If there is nothing before the cutoff, keep it all (icut = 0) and
        #          make sure to also keep at least one unless that one is delete.
        #          If it is delete, it will be covered in step 2
        # Step 1d: Keep ~~everything~~ (NO) to the right of icut. Universal set
        #          Only keep references to the right.
        # Step 1e: Save for deletetion group to the left of icut. Grouped list.
        #          Make sure to include ref_rpaths too. They will always get deleted.
        # Step 1f: If subdir, add in additional references to keep that could point
        #          there from outside only
        keep_rpaths = set()
        del_groups = {}
        for name, group in groups:
            iwhen = keyed_bisect_right(group, when, key=itemgetter("timestamp"))  # 1a
            iwhen -= keep  # 1b
            if iwhen >= len(group) and group[-1]["size"] < 0:
                icut = iwhen
            else:
                icut = max([iwhen - 1, 0])  # 1a,c
                icut = min([icut, len(group) - 1])

            keep_rpaths.update(
                row["rpath"] for row in group[icut:] if row.get("ref_rpath", None)
            )  # 1d
            logger.debug(f'(1) {name!r} keep {[row["rpath"] for row in group[icut:]]}')

            del_groups[name] = group[:icut]  # 1e
            r0 = [row["rpath"] for row in group[:icut]]
            r1 = [row.get("ref_rpath", None) for row in group[:icut]]
            logger.debug(f"(1) {name!r} consider for del {r0 + list(filter(bool,r1))}")

        if subdir:  # 1f
            with self.db() as db:
                res = db.execute(
                    """
                    SELECT rpath
                    FROM items
                    WHERE
                        rpath LIKE ? -- points inside subdir
                    AND
                        apath NOT LIKE ? -- but ins't inside it
                    """,
                    (subdir + "%", subdir + "%"),
                )
                keep_rpaths.update(row["rpath"] for row in res)

        # Step 2:  Loop over each group of to-be-deleted files
        # Step 2a: Delete everything that isn't (1) referenced (i.e. in keep_rpaths)
        #          or (2) a delete marker. Includes ref_rpath; which is not rpath
        #          but will delete the reference file and do nothing when applied to the
        #          DB (but the actual rpath will have been deleted too)
        # Step 2b: Remove any delete markers if and only if not the last item since
        #          they aren't hiding anything except maybe the final.
        # Step 2c: If and only if there is only ONE remaining item and it is a delete
        #          marker, delete it. (This makes sure that a delete marker "hides"
        #          a kept file, that is kept because of reference)
        del_rpaths = set()
        for name, group in del_groups.items():  # only with subdir from step 0
            _d = set()  # This isn't needed but makes debug msg easier
            keep_group = []
            for row in group:  # 2a
                if rr := row.get("ref_rpath", None):
                    _d.add((rr, 0))

                if row["rpath"] in keep_rpaths or row["size"] < 0:  # (1) & (2)
                    keep_group.append(row)  # do not delete yet
                    continue

                _d.add((row["rpath"], row["size"]))

            del_rpaths.update(_d)

            logger.debug(
                f'(2a) {name!r} temp keep {[row["rpath"] for row in keep_group]}'
            )
            logger.debug(f"(2a) {name!r} del {_d}")

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

            logger.debug(
                f'(2b) {name!r} temp keep {[row["rpath"] for row in still_keep]}'
            )
            logger.debug(f"(2b) {name!r} del {_d}")

            # 2c
            if len(still_keep) > 1:
                continue
            row = still_keep[0]
            if row["size"] < 0:
                del_rpaths.add((row["rpath"], row["size"]))

                logger.debug(f'(2c) {name!r} del {row["rpath"]!r}')

        # A note: This can be made even more agressive because this may leave behind an
        #         unneeded delete marker. For the most part, this is ignore but see
        #         if there are any

        # # Not used yet. Needs to be more thought out and tested
        # res = self.db().execute("""
        #     SELECT rpath
        #     FROM (
        #         -- Nest the conditional so that it finds all initials and then
        #         -- later takes deletes.
        #         SELECT rpath,size
        #         FROM items
        #         GROUP BY apath
        #         HAVING MIN(timestamp) -- NOT max
        #     )
        #     WHERE size <0""")
        # for row in res:
        #     del_rpaths.add((row["rpath"], -1))

        # This section has 100% test coverage! Very good to have with the complexities
        return del_rpaths

    def delete_rpath(self, rpath):
        """
        Remove all entries that point to the rpath even if they are references
        """
        db = self.db()
        item = {"_V": 1, "_action": "prune", "rpath": rpath}

        with db:
            # Get the implicit rowid and then use that for the delete so it only scans
            # once. rowid is implicit and indexed.
            row = db.execute(
                """
                SELECT rowid,size
                FROM items
                WHERE rpath = ?
                LIMIT 1
                """,
                (rpath,),
            ).fetchone()

            try:
                item["size"] = row["size"]

                db.execute(
                    """
                    DELETE FROM items
                    WHERE rowid = ?
                    """,
                    (row["rowid"],),
                )
            except:
                # Likely means that there was not item that matched but this is more robust
                # than just checking
                pass

        with self.snap_file.open(mode="at") as fp:
            print(json.dumps(item), file=fp, flush=True)

        return rpath


class BrokenReferenceError(ValueError):
    pass


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
