"""
Main backup object. This will list the source (and reset/list the dest if --refresh)
then compare.
"""
import sys, os
import time
import json
import tempfile
import subprocess
import shlex
import atexit
import shutil
import queue
import logging
import gzip as gz
from collections import defaultdict
from textwrap import dedent
from threading import Thread
from functools import partial

from . import LOCK, MIN_RCLONE
from .dstdb import DFBDST, apath2rpath
from .rclonerc import IGNORED_FILE_DATA, rcpathjoin
from .threadmapper import ReturnThread, thread_map_unordered as tmap
from .utils import (
    star,
    human_readable_bytes,
    shell_runner,
    time_format,
    listify,
    smart_open,
)

# For testing only
from . import _FAIL

logger = logging.getLogger(__name__)

_r = repr


class NoCommonHashError(ValueError):
    pass


class LinkError(ValueError, OSError):
    pass


class Backup:
    def __init__(self, config):
        self.t0 = time.time()
        self.config = config
        self.errcount = 0

    def run(self):
        config = self.config
        cliconfig = config.cliconfig

        config._set_auto()

        self.call_shell(mode="pre")

        self.src_rclone = config._config["src_rclone"]

        ver = self.config.rc.call("core/version")
        logger.info("rclone version: " + ".".join(str(i) for i in ver["decomposed"]))
        for k, v in ver.items():
            logger.debug(f"   {k}: {v}")

        ver = tuple(int(c) for c in self.src_rclone.version_dict["decomposed"])
        if ver < MIN_RCLONE:
            raise ValueError(
                "Unsupported rclone version. "
                f"Must use {'.'.join(f'{i}' for i in MIN_RCLONE)} or newer"
            )

        self.dstdb = DFBDST(config)

        # Step 1: List Files locally and maybe on remote
        self.list_files()  # self.src_files, self.dst_files

        # Step 2: Compare
        self.compare()  # sets new, modified, deleted, and update_dstdb

        # update dstdb for files that match on dst_compare so that they can
        # use [src]compare next time.
        self.dstdb.replace_many(self.update_dstdb)

        # Step 3: Move Tracking
        self.track_moves()  # updates new, deleted and adds moves (original_dfile,moved_sfile)

        self.action_summary()

        if cliconfig.dry_run:
            logger.info("DRY-RUN. Exit")
            return
        elif cliconfig.interactive:
            r = input("Do you want to continue? [Y]/N:")
            if r.lower().startswith("n"):
                return

        self.dump = []

        # Step 4: Transfers. If --dump, will not act but will populate self.dump
        self.transfer()
        self.reference() if config.rename_method == "reference" else self.move_by_copy()
        self.delete()

        if file := cliconfig.dump:
            try:
                fp = smart_open(file, "wt") if file != "-" else sys.stdout
                for item in self.dump:
                    print(
                        json.dumps(item, ensure_ascii=False, separators=(",", ":")),
                        file=fp,
                        flush=True,
                    )
            finally:
                if file != "-":
                    fp.close()
                    logger.info(f"Written to {_r(file)}")
            return

        stats = self.run_stats()
        logger.info("-----")
        for line in stats.split("\n"):
            logger.info(line)
        logger.info("-----")

        self.call_shell(mode="post", stats=stats)

        if not cliconfig.dry_run:
            self.upload_snapshots()
            self.upload_logs()

    def list_files(self, stats=None):
        """List the source and refresh dest if needed"""
        config = self.config
        kwargs = dict(stats=stats or self.config.stats)

        if self.config.cliconfig.refresh:
            sthread = ReturnThread(target=self.list_src, kwargs=kwargs).start()
            dthread = ReturnThread(
                target=self.dstdb.reset,
                kwargs=kwargs | {"use_snapshots": config.cliconfig.use_snapshots},
            ).start()
            source_files = sthread.join()
            dthread.join()
        else:
            logger.info("Listing source")
            source_files = self.list_src(**kwargs)

        self.src_files = {file["apath"]: file for file in source_files}

        d = self.dstdb.snapshot(path=config.cliconfig.subdir)
        d = (self.dstdb.fullrow2dict(row) for row in d)
        self.dst_files = {file["apath"]: file for file in d}

        logger.info(f"Found {len(self.src_files)} source Files")
        logger.info(f"Found {len(self.dst_files)} dest Files")

    def list_src(self, stats=None):
        config = self.config

        fsroot = config.rc.features(config.src).get("Root", "")
        if fsroot and not os.path.exists(fsroot):
            fsroot = ""

        flags = []

        compute_hashes = (
            config.get_hashes or config.compare == "hash" or config.renames == "hash"
        )

        modtime = (
            config.get_modtime
            or config.compare == "mtime"
            or config.dst_compare == "mtime"
            or config.renames == "mtime"
        )

        logger.debug(f"{compute_hashes = }, {modtime = }")

        hash_flags = []
        if compute_hashes:
            hash_flags.append("--hash")
            for htype in listify(config.hash_type):
                hash_flags.extend(["--hash-type", htype])

        subdir = config.cliconfig.subdir or ""  # Make it empty instead of None
        if subdir:
            msg = f"subdir {_r(subdir)} specified. Filters may break!"
            logger.warning(msg)

        if config.links == "link":
            flags.append("--links")
        elif config.links == "skip":
            flags.append("--skip-links")
        else:  # == 'copy'
            pass  # Already dealt with in config

        rcfiles = config.src_rclone.listremote(
            filter_flags=config.filter_flags,
            # fast_list=... # Would be in rclone_flags. Already set
            mimetype=False,
            modtime=modtime,
            metadata=config.metadata,
            only="files",
            epoch_time=True,
            flags=flags + hash_flags,
            subdir=subdir,
        )

        files = []
        t0 = time.time()
        c = 0
        for file in rcfiles:
            c += 1
            new = {
                "apath": os.path.join(subdir, file.pop("Path")),
                "size": file.pop("Size"),
                "mtime": file.pop("ModTime", None),
            }

            if hashes := file.pop("Hashes", None):
                new["checksum"] = hashes

            if new["apath"].endswith(".rclonelink") and config.links == "link":
                new["linkdata"] = link = {}
                link["real_apath"] = new["apath"].removesuffix(".rclonelink")
                try:
                    lfull = os.path.join(fsroot, link["real_apath"])
                    link["link_dest"] = os.readlink(lfull)
                except OSError:
                    m = f"{_r(link['real_apath'])} could not be read."
                    if os.path.islink(link["real_apath"]):
                        raise LinkError(m)
                    logger.debug(m + " Treating as a file")
                    del new["linkdata"]  # To treat as a file

            for k, v in file.items():
                if k in IGNORED_FILE_DATA:
                    continue
                new[k] = v

            files.append(new)
            if stats and (time.time() - t0) >= stats:  # TODO TEST
                logger.info(f"Source Listing Status: {c} items")
                t0 = time.time()

        # Testing
        if "missing_hashes" in _FAIL:
            for file in files:
                file.pop("checksum", None)
        # end testing

        logger.debug(f"Listed {len(files)} files")
        return files

    def compare(self):
        config = self.config

        self.new = []
        self.modified = []
        self.deleted = list(set(self.dst_files) - set(self.src_files))
        self.update_dstdb = []

        for apath, sfile in self.src_files.items():
            try:
                dfile = self.dst_files[apath]
            except KeyError:
                self.new.append(apath)
                continue

            if not self.file_compare(sfile, dfile):
                self.modified.append(apath)
                continue

            # They match! But see if we need to update the dstdb with the better
            # information at source. This enables things like using mtime for
            # source-to-source but not for source-to-dest
            if dfile["dstinfo"]:
                logger.debug(f"Updating {_r(apath)} with src info")
                new = dfile.copy()
                new.update(sfile)
                new["dstinfo"] = 0
                self.update_dstdb.append(new)

    def file_compare(self, sfile, dfile, attrib=None):
        config = self.config

        attrib = attrib or (
            config.dst_compare if dfile["dstinfo"] else self.config.compare
        )

        msg = [f"Compare {_r(sfile['apath'])} with {attrib = }."]
        try:
            s = sfile.get("size", "src_missing_size")
            d = dfile.get("size", "dst_missing_size")
            if s != d:
                msg.append(f"Mismatch sizes. src: {s}, dst: {d}.")
                return False

            if attrib == "mtime":
                s = sfile.get("mtime", "src_missing_mtime")
                d = dfile.get("mtime", "dst_missing_mtime")
                try:
                    c = abs(s - d) < config.dt
                except TypeError:
                    c = False
                if not c:
                    msg.append(f"Mismatch mtime. src: {s}, dst: {d}.")
                    return False

            if attrib == "hash":
                scheck = sfile.get("checksum", {}) or {}  # Nones to empty dict
                dcheck = dfile.get("checksum", {}) or {}

                # This is a different case than no shared hashes. This happens when a remote
                # doesn't return the hashes. Like rclone itself [1,2]. Ideally, we would
                # have a settable fallback such as ModTime but this happens after listing
                # and we don't want to have to list all ModTimes on the off chance of a
                # fallback
                #     [1] https://rclone.org/flags/
                #         "-c, --checksum  Skip based on checksum (if available) & size,
                #         not mod-time & size"
                #     [2] https://forum.rclone.org/t/behavior-of-rclone-when-checksum-
                #         but-checksum-is-missing-is-undocumented-and-unexpected/39231/3
                #
                if (not scheck or not dcheck) and not config.error_on_missing_hash:
                    logger.info(f"WARNING: Missing hashes on source and/or dest")
                    logger.info(f"             src: {_r(sfile['apath'])}")
                    logger.info(f"             dst: {_r(dfile['rpath'])}")
                    logger.info(f"         Reverting to 'size' only")

                shared_hashes = set(scheck).intersection(set(dcheck))
                if not shared_hashes and config.error_on_missing_hash:
                    m = "Non compatible (or non existent) hashes. Change attributes"
                    logger.info(m)
                    msg.append(m)
                    msg.append(f"source = {list(scheck)}, dest = {list(dcheck)}")
                    raise NoCommonHashError(m)

                for hashname in shared_hashes:
                    if scheck[hashname] != dcheck[hashname]:
                        msg.append(f"Checksum {hashname} does not match")
                        return False

            msg.append("MATCH")
            return True
        finally:
            msg = " ".join(msg)
            logger.debug(msg)

    def track_moves(self):
        renames = self.config.renames
        dst_renames = self.config.dst_renames

        self.moves = []

        if not self.deleted or not self.new:
            logger.info("No new *and* deleted files. No rename tracking")
            return

        # The algorithm for this is pretty simple. When a file is
        # renamed, it looks like the old file is deleted and the
        # new file is created. So the candidates are pretty simple.
        #
        # Since size must *always* match, we make a dictionary by sizes
        # to reduce the pool

        del_by_size = defaultdict(list)
        for apath in self.deleted:
            dfile = self.dst_files[apath]
            del_by_size[dfile["size"]].append(dfile)

        for apath in self.new:
            sfile = self.src_files[apath]

            dfiles0 = del_by_size[sfile["size"]]  # list of candidate paths
            dfiles = []
            for dfile in dfiles0:
                # Note that in the config dst_renames is already set to the correct
                # values if it was None
                attrib = dst_renames if dfile.get("dstinfo", 0) else renames
                if not attrib:
                    continue

                if self.file_compare(sfile, dfile, attrib=attrib):
                    dfiles.append(dfile)

            if len(dfiles) == 1:
                self.moves.append((dfiles[0], sfile))  # dfile,moved sfile
            elif not dfiles:
                logger.debug(f"no moves for deleted file {_r(apath)}")
                continue
            else:
                logger.info(f"Too many matches for {_r(apath)}. Not moving")

        # Now we need to remove the moves from new and delete
        undelete = set()
        unnew = set()
        for dfile, moved_sfile in self.moves:
            undelete.add(dfile["apath"])
            unnew.add(moved_sfile["apath"])

        self.new[:] = list(set(self.new) - unnew)
        # DO NOT UNDELETE!!! We still want them to be "deleted" with a delete marker
        # NO: self.deleted[:] = list(set(self.deleted) - undelete)

    def transfer(self):
        config = self.config
        # dst_rclone = self.config.dst_rclone

        # The upload pipeline is done in a functional programing(esque) fashion
        # to better enable concurrency.

        comb = self.new + self.modified
        N = len(comb)
        apaths = iter(comb)

        def _apath2file(apath):
            ts = self.config.now.ts

            file = self.src_files[apath].copy()
            file["rpath"] = rpath = apath2rpath(file["apath"], ts)
            file["timestamp"] = ts
            file["dstinfo"] = False  # Since this is coming from the source
            return file

        files = map(_apath2file, apaths)

        if config.cliconfig.dump:
            self.dump.extend(files)
            return

        rc = self.config.rc
        rc.start()

        def _transfer(file):
            try:
                sfile = self.config.src, file["apath"]
                dfile = self.config.dst, file["rpath"]

                link = file.get("linkdata", None)

                msg = f"Uploading {_r(file['apath'])} to {_r(file['rpath'])}"

                logger.info(msg)

                if link:
                    rc.write(dfile, link["link_dest"], _config={"NoCheckDest": True})
                    m = f"apath = {_r(file['apath'])} is a LINK to {_r(link['link_dest'])}"
                    logger.debug(m)
                else:
                    rc.copyfile(
                        src=sfile,
                        dst=dfile,
                        _config={
                            "NoCheckDest": True,
                            "metadata": self.config.metadata,
                        },
                    )
                return file
            except Exception as EE:
                logger.error(f"Upload Error: {_r(file['apath'])}. {EE}")
                with LOCK:
                    self.errcount += 1

        files = tmap(_transfer, files, Nt=config.concurrency)
        files = filter(bool, files)
        # We could theoretically do an insert_many but that could lock the DB and/or
        # require we accumulate. Instead, let it go right to insert which closes the DB
        # each time. This is trivial compared to upload times. Plus, we want an upload
        # to be recorded in the DB right away
        files = map(self.dstdb.insert, files)

        stats = StatsThread(self.config, N=N, daemon=True).start()

        # Make them work
        for file in files:
            stats += 1

        stats.join()

    def reference(self):
        config = self.config

        # The upload pipeline is done in a functional(esque) fashion
        # to better enable concurrency.
        moves = iter(self.moves)

        # Moves are already paired as original_dfile,moved_sfile
        def _build_new_file(original_dfile, moved_sfile):
            ts = self.config.now.ts

            new = original_dfile.copy()
            new.update(moved_sfile)

            new["original"] = original_dfile["apath"]
            new["isref"] = True
            new["rpath"] = original_dfile["rpath"]
            new["ref_rpath"] = apath2rpath(new["apath"], ts, flag="R")
            new["timestamp"] = ts
            new["dstinfo"] = False  # Since this is coming from the source
            return new

        files = map(star(_build_new_file), moves)

        if config.cliconfig.dump:
            self.dump.extend(files)
            return

        rc = self.config.rc
        rc.start()

        def _upload_ref(file):
            original = file["original"]
            ref_rpath = file["ref_rpath"]
            rpath = file["rpath"]

            ref = {
                "ver": 2,
                "rel": os.path.relpath(rpath, os.path.dirname(ref_rpath)),
            }
            reftxt = json.dumps(ref)
            try:
                logger.info(
                    f"Moving {_r(original)} to "
                    f"{_r(file['apath'])} with "
                    f"{_r(ref_rpath)}."
                )
                rc.write(
                    (config.dst, ref_rpath),
                    reftxt,
                )
                return file
            except Exception as EE:
                logger.error(f"Reference Error: {_r(file['apath'])}. {EE}")
                with LOCK:
                    self.errcount += 1

        files = tmap(_upload_ref, files, Nt=config.concurrency)
        files = filter(bool, files)
        files = map(self.dstdb.insert, files)

        # Make them work
        for file in files:
            pass

    def move_by_copy(self):
        config = self.config

        # The upload pipeline is done in a functional programing(esque) fashion
        # to better enable concurrency.
        moves = iter(self.moves)

        def _build_copiedfile(original_dfile, moved_sfile):
            ts = self.config.now.ts

            new = original_dfile.copy()
            new.update(moved_sfile)

            new["original"] = original_dfile["apath"]
            new["rpath"] = apath2rpath(new["apath"], ts)
            new["source_rpath"] = original_dfile["rpath"]
            new["timestamp"] = ts
            new["isref"] = False
            new["dstinfo"] = False  # Since this is coming from the source
            return new

        files = map(star(_build_copiedfile), moves)

        if config.cliconfig.dump:
            self.dump.extend(files)
            return

        rc = self.config.rc
        rc.start()

        def _copy(file):
            try:
                msg = f'"Moving" {_r(file["original"])} to {_r(file["apath"])} via copy'

                sfile = rcpathjoin(self.config.dst, file["source_rpath"])
                dfile = rcpathjoin(self.config.dst, file["rpath"])

                logger.info(msg)

                rc.copyfile(
                    src=sfile,
                    dst=dfile,
                    _config={
                        "NoCheckDest": True,
                        "metadata": self.config.metadata,
                    },
                )
                return file
            except Exception as EE:
                logger.error(f"Copy Error: {_r(file['apath'])}. {EE}")
                with LOCK:
                    self.errcount += 1

        files = tmap(_copy, files, Nt=config.concurrency)
        files = filter(bool, files)
        files = map(self.dstdb.insert, files)

        # Make them work
        for file in files:
            pass

    def delete(self):
        config = self.config
        # dst_rclone = self.config.dst_rclone

        # The upload pipeline is done in a functional programing(esque) fashion
        # to better enable concurrency.
        apaths = iter(self.deleted)

        def _apath2file(apath):
            ts = self.config.now.ts

            file = self.dst_files[apath].copy()
            file["rpath"] = rpath = apath2rpath(file["apath"], ts, flag="D")
            file["timestamp"] = ts
            file["dstinfo"] = False  # Since this is coming from the source
            file["size"] = -1
            return file

        files = map(_apath2file, apaths)

        if config.cliconfig.dump:
            self.dump.extend(files)
            return

        rc = self.config.rc
        rc.start()

        def _delete(file):
            dfile = file["rpath"]
            try:
                logger.info(f"Deleting {_r(file['apath'])} with {_r(dfile)}.")
                rc.write((config.dst, dfile), b"DEL")
                return file
            except Exception as EE:
                logger.error(f"Delete Error: {_r(file['apath'])}. {EE}")
                with LOCK:
                    self.errcount += 1

        files = tmap(_delete, files, Nt=config.concurrency)
        files = filter(bool, files)
        files = map(self.dstdb.insert, files)

        # Make them work
        for file in files:
            pass

    def action_summary(self):
        self.action_summary_text = []

        _p = logger.debug
        if self.config.cliconfig.dry_run or self.config.cliconfig.interactive:
            _p = logger.info

        m = f"New: {self.summary(self.new)}"
        self.action_summary_text.append(m)
        logger.info(m)
        for file in self.new:
            _p(f"   {_r(file)}")

        m = f"Modified: {self.summary(self.modified)}"
        self.action_summary_text.append(m)
        logger.info(m)
        for file in self.modified:
            _p(f"   {_r(file)}")

        m = f"Deleted: {self.summary(self.deleted,src=False)}"
        self.action_summary_text.append(m)
        logger.info(m)
        for file in self.deleted:
            _p(f"   {_r(file)}")

        m = f"Moves: {self.summary([f[0]['apath'] for f in self.moves],src=False)}"
        self.action_summary_text.append(m)
        logger.info(m)
        for file in self.moves:
            _p(f"   {_r(file[0]['apath'])} --> {_r(file[1]['apath'])}")

    def summary(self, files, src=True):
        flist = self.src_files if src else self.dst_files
        size = sum(flist[file]["size"] for file in files)
        num, units = human_readable_bytes(size)
        s = "s" if len(files) != 1 else ""
        return f"{len(files)} file{s} ({num:0.2f} {units})"

    def run_stats(self):
        stats = [f"Errors: {self.errcount}"]
        select = """
            SUM(CASE 
                WHEN (size >= 0 AND (isref IS NULL OR isref = 0) )
                THEN size ELSE 0 END) 
            AS totsize,
            COUNT(size) as num
            """
        cur = self.dstdb.snapshot(select=select).fetchone()
        num, units = human_readable_bytes(cur["totsize"])
        s = "s" if cur["num"] != 1 else ""
        stats.append(f"Current {cur['num']} file{s} ({num:0.2f} {units})")

        tot = self.dstdb.db().execute(f"SELECT {select} FROM items").fetchone()
        num, units = human_readable_bytes(tot["totsize"])
        s = "s" if tot["num"] != 1 else ""
        stats.append(f"Total {tot['num']} file{s} ({num:0.2f} {units})")

        stats.extend(self.action_summary_text)
        stats.append(f"Elapsed Time (approx): {time_format(time.time() - self.t0)}")
        return "\n".join(stats)

    def call_shell(self, *, mode, stats=""):
        dry = self.config.cliconfig.dry_run

        if mode == "pre":
            cmds = self.config.pre_shell
        elif mode == "post":
            cmds = self.config.post_shell

        if not cmds:
            logger.debug(f"No cmds for {mode = }")
            return

        env = {
            "CONFIGDIR": self.config._config["__dir__"],
            "STATS": stats,
            "ERRS": str(self.errcount),
        }

        returncode = shell_runner(cmds, dry=dry, env=env, prefix=f"{mode}.shell")

        if returncode and self.config.stop_on_shell_error:
            raise subprocess.CalledProcessError(returncode, cmds)

    def upload_logs(self):
        config = self.config

        if not config.logfile.exists():
            return

        name = f"{self.config.now.dt}Z.log"
        log_dests = [rcpathjoin(l, name) for l in listify(config.log_dest)]

        log_dests.append((config.dst, f".dfb/logs/{name}"))

        if not log_dests:
            logger.debug("no log destinations")
            return

        # Need to copy the log file since it may change in the process of the upload
        # from the calls itself
        log_copy = config.logfile.with_stem("log_copy")
        shutil.copy2(config.logfile, log_copy)

        for log_dest in log_dests:
            dtxt = rcpathjoin(*listify(log_dest))
            logger.info(f"Uploading log to {_r(dtxt)}")
            try:
                config.rc.copyfile(
                    src=log_copy,
                    dst=log_dest,
                )
            except Exception as e:
                logger.error(f"Failed: {e}")

    def upload_snapshots(self):
        name = f"{self.config.now.dt}Z.jsonl"
        snap_src0 = self.config.tmpdir / name

        if not snap_src0.exists() or not snap_src0.stat().st_size:
            return

        snap_srcz = self.config.tmpdir / f"{name}.gz"

        with gz.open(str(snap_srcz), "wb") as fz, snap_src0.open("rb") as fu:
            while block := fu.read(3 * 1024 * 1024):  # 3 MiB
                fz.write(block)

        self.config.rc.copyfile(
            src=snap_srcz,
            dst=(
                self.config.dst,
                # Upload to dated dirs too so that they don't fill with too many
                # files if this is run often.
                f".dfb/snapshots/{self.config.now.obj.strftime('%Y/%m')}/{name}.gz",
            ),
            _config={"NoCheckDest": True},
        )


class StatsThread(Thread):
    def __init__(self, config, N, *args, **kwargs):
        self.config = config
        self.N = N
        self.fcount = 0

        # Rather than a while loop with a time.sleep and a conditional,
        # instead use a queue with a timeout. This means we can put something
        # in the queue to kill it right away. (Can this use events instead?)
        self.stop = queue.Queue()

        super().__init__(*args, **kwargs)

    def start(self, *args, **kwargs):
        super().start(*args, **kwargs)
        return self

    def increment(self, n=1):
        with LOCK:
            self.fcount += n
        return self

    __iadd__ = increment  # += n

    def run(self):
        self.config.rc.call("core/stats-reset")

        while True:
            try:
                stop = self.stop.get(block=True, timeout=self.config.stats)
                if stop:
                    break
            except queue.Empty:
                pass

            # Get the average speed. But use our own measure of totals
            stats = self.config.rc.call("core/stats")
            speednum, speedunits = human_readable_bytes(stats["speed"])
            totnum, totunits = human_readable_bytes(stats["totalBytes"])
            dt = time_format(stats["elapsedTime"])

            msg = [f"STATS: Elapsed {dt};"]
            msg.append(f"Transfering {len(stats.get('transferring',0))};")
            msg.append(f"Avg. Speed {speednum:0.2f} {speedunits}/sec;")
            # stats['totalTransfers'] includes active so use self.fcount

            msg.append(f"Total {self.fcount}/{self.N} ({totnum:0.2f} {totunits})")
            logger.info(" ".join(msg))

    def join(self, *a, **k):
        self.stop.put(True)
        super().join(*a, **k)
        logger.debug("Joined stats thread")
