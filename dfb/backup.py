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
from collections import defaultdict
from textwrap import dedent
from threading import Thread
from functools import partial


from . import debug, log, LOCK, MIN_RCLONE
from .dstdb import DFBDST, apath2rpath
from .rclone import IGNORED_FILE_DATA, rcpathjoin
from .checksumdb import SourceChecksumDB
from .threadmapper import ReturnThread, thread_map_unordered as tmap
from .utils import (
    star,
    bytes2human,
    shell_runner,
    time_format,
    shell_header,
    listify,
)

# For testing only
from . import _FAIL


_r = repr


class NoCommonHashError(ValueError):
    pass


class LinkError(ValueError, OSError):
    pass


class Backup:
    def __init__(self, config):
        self.t0 = time.time()
        self.config = config
        self.shell_out = []

    def run(self):
        config = self.config
        cliconfig = config.cliconfig
        self.call_shell(mode="pre")

        self.src_rclone = config._config["src_rclone"]

        log("rclone version:")
        log(self.src_rclone.version.decode(), prefix="rclone")

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
            log("DRY-RUN. Exit")
            return
        elif cliconfig.interactive:
            r = input("Do you want to continue? [Y]/N:")
            if r.lower().startswith("n"):
                return

        if cliconfig.shell_script:
            out = self.shell_out

            out.append("# dfb shell script output")

            for line in SHELL_SCRIPT_WARNING.split("\n"):
                out.append(shlex.join(["echo", line]))
            log(SHELL_SCRIPT_WARNING)

            out.append("## Environment")
            out.append(shell_header(config, cd=True))

            out.append("\n## Transfers")
            out.append(self.transfer(shell_script=True))

            if config.rename_method == "reference":
                out.append("\n## References (moves)")
                out.append(self.reference(shell_script=True))
            else:
                out.append("\n## Copies (moves)")
                out.append(self.move_by_copy(shell_script=True))

            out.append("\n## Deletes")
            out.append(self.delete(shell_script=True))

            if cliconfig.shell_script == "-":
                log.print("\n".join(out), flush=True)
            else:
                with open(cliconfig.shell_script, "wt") as fp:
                    fp.write("\n".join(out))
                log(f"Shell script written to {_r(cliconfig.shell_script)}")
            return

        err = []
        # Step 4: Transfer
        try:
            self.transfer()
        except ValueError:
            err.append("transfer")

        if config.rename_method == "reference":
            try:
                self.reference()
            except ValueError:
                err.append("reference")
        else:
            try:
                self.move_by_copy()
            except ValueError:
                err.append("move_by_copy")

        try:
            self.delete()
        except ValueError:
            err.append("delete")

        if err:
            raise ValueError(f"Error(s) occured in {', '.join(err)}")

        stats = self.run_stats()
        log("-----")
        log(stats)
        log("-----")

        self.call_shell(mode="post", stats=stats)

        if not cliconfig.dry_run or not cliconfig.interactive:
            self.upload_logs()

    def list_files(self, stats=None):
        """List the source and refresh dest if needed"""
        config = self.config
        kwargs = dict(stats=stats or self.config.stats)

        if self.config.cliconfig.refresh:
            sthread = ReturnThread(target=self.list_src, kwargs=kwargs).start()
            dthread = ReturnThread(target=self.dstdb.reset, kwargs=kwargs).start()
            source_files = sthread.join()
            dthread.join()
        else:
            log("Listing source")
            source_files = self.list_src(**kwargs)

        self.src_files = {file["apath"]: file for file in source_files}

        d = self.dstdb.snapshot(path=config.cliconfig.subdir)
        d = (self.dstdb.fullrow2dict(row) for row in d)
        self.dst_files = {file["apath"]: file for file in d}

        log(f"Found {len(self.src_files)} source Files")
        log(f"Found {len(self.dst_files)} dest Files")

    def list_src(self, stats=None):
        config = self.config

        fsroot = config.rc.features(config.src).get("Root", "")
        if fsroot and not os.path.exists(fsroot):
            fsroot = ""

        flags = []

        compute_hashes = any(
            [config.get_hashes, config.compare == "hash", config.renames == "hash"]
        )

        modtime = (
            config.get_modtime
            or config.compare == "mtime"
            or (config.cliconfig.refresh and config.dst_compare == "mtime")
            or config.renames == "mtime"
            or (compute_hashes and config.reuse_hashes == "mtime")
        )

        if compute_hashes:
            hash_flags = ["--hash"]
            if config.hash_type:
                if isinstance(config.hash_type, str):
                    config.hash_type = [config.hash_type]
                for htype in config.hash_type:
                    hash_flags.extend(["--hash-type", htype])

        debug(f"{compute_hashes = }, {modtime = }")

        subdir = config.cliconfig.subdir or ""  # Make it empty instead of None
        if subdir:
            msg = f"WARNING: subdir {_r(subdir)} specified. Filters may break!"
            log(msg)

        if config.links == "link":
            flags.append("--links")
        elif config.links == "skip":
            flags.append("--skip-links")
        else:  # == 'copy'
            pass  # Already dealt with in config

        hf0 = hash_flags if (compute_hashes and not config.reuse_hashes) else []

        rcfiles = config.src_rclone.listremote(
            filter_flags=config.filter_flags,
            # fast_list=... # Would be in rclone_flags. Already set
            mimetype=False,
            modtime=modtime,
            metadata=config.metadata,
            only="files",
            epoch_time=True,
            # This shouldn't be needed and I should use hashes and hashtypes but they
            # do not work. I'll look into fixing that later but for now, if it ain't broke...
            flags=flags + hf0,
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
                    debug(m + " Treating as a file")
                    del new["linkdata"]  # To treat as a file

            for k, v in file.items():
                if k in IGNORED_FILE_DATA:
                    continue
                new[k] = v

            files.append(new)
            if stats and (time.time() - t0) >= stats:  # TODO TEST
                log(f"Source Listing Status: {c} items")
                t0 = time.time()

        # Testing
        if "missing_hashes" in _FAIL:
            for file in files:
                file.pop("checksum", None)
        # end testing

        debug(f"Listed {len(files)} files")
        if not compute_hashes or (compute_hashes and not config.reuse_hashes):
            debug("No need to compute checksums or already computed. Done")
            return files  # No hashes or already added

        ## At this point, we need to add checksums and save it

        checksumdb = SourceChecksumDB(config)

        for file in files:
            checksumdb.add_checksum_to_file(file)

        # Determine which ones need a checksum and make them into a dict
        # refereced by apath for quick lookup
        wo_checksum = {}
        for file in files:
            if file.get("checksum", None):
                continue
            wo_checksum[file["apath"]] = file
        log(f"Found {len(wo_checksum)} without checksums. Recomputing")

        with tempfile.NamedTemporaryFile(mode="w+t") as fp:
            fp.write("\n".join(wo_checksum))
            fp.flush()
            updated = config.src_rclone.listremote(
                filter_flags=["--files-from", fp.name],
                # fast_list=... # Would be in rclone_flags. Already set
                mimetype=False,
                modtime=False,
                metadata=False,
                only="files",
                # epoch_time=True,
                flags=flags + hash_flags,
            )

            # Update the item. This will update in files too
            for upfile in updated:
                hashes = upfile.get("Hashes", None)
                if not hashes:
                    debug(f"Missing hashes from {upfile}")
                wo_checksum[upfile["Path"]]["checksum"] = hashes

        # Update the DB
        checksumdb.update_db(wo_checksum.values())

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
                debug(f"Updating {_r(apath)} with src info")
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
                scheck = sfile.get("checksum", {})
                dcheck = dfile.get("checksum", {})

                scheck = scheck or {}  # Nones to empty dict
                dcheck = dcheck or {}

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
                    msg = [
                        "WARNING: Missing hashes on source and/or dest",
                        f"             src: {_r(sfile['apath'])}",
                        f"             dst: {_r(dfile['rpath'])}",
                        "         Reverting to 'size' only",
                    ]
                    log("\n".join(msg))

                shared_hashes = set(scheck).intersection(set(dcheck))
                if not shared_hashes and config.error_on_missing_hash:
                    m = "Non compatible (or non existent) hashes. Change attributes"
                    log(m)
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
            debug(msg)

    def track_moves(self):
        renames = self.config.renames
        dst_renames = self.config.dst_renames

        self.moves = []

        if not self.deleted or not self.new:
            log("No new *and* deleted files. No rename tracking")
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
                debug(f"no moves for deleted file {_r(apath)}")
                continue
            else:
                log(f"Too many matches for {_r(apath)}. Not moving")

        # Now we need to remove the moves from new and delete
        undelete = set()
        unnew = set()
        for dfile, moved_sfile in self.moves:
            undelete.add(dfile["apath"])
            unnew.add(moved_sfile["apath"])

        self.new[:] = list(set(self.new) - unnew)
        # DO NOT UNDELETE!!! We still want them to be "deleted" with a delete marker
        # NO: self.deleted[:] = list(set(self.deleted) - undelete)

    def transfer(self, shell_script=False):
        config = self.config
        # dst_rclone = self.config.dst_rclone

        # The upload pipeline is done in a functional programing(esque) fashion
        # to better enable concurrency.
        self.errcount = 0

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

        if shell_script:
            out = []
            cmd = [config.rclone_exe] + config.rclone_flags
            cmd.extend(["copyto", "--no-check-dest"])

            for file in files:
                sfile = rcpathjoin(self.config.src, file["apath"])
                dfile = rcpathjoin(self.config.dst, file["rpath"])

                link = file.get("linkdata", None)

                if link:
                    echo = shlex.join(["printf", link["link_dest"]])
                    cmdt = cmd.copy()
                    cmdt[cmdt.index("copyto")] = "rcat"
                    rcat = shlex.join(cmdt + [dfile])
                    out.append(f"{echo} | {rcat}")
                else:
                    out.append(shlex.join(cmd + [sfile, dfile]))

            return "\n".join(out)

        rc = self.config.rc
        rc.start_rc()

        def _transfer_rc(file, *, rc):
            try:
                sfile = self.config.src, file["apath"]
                dfile = self.config.dst, file["rpath"]

                link = file.get("linkdata", None)

                msg = f"Uploading {_r(file['apath'])} to {_r(file['rpath'])}"

                log(msg)

                if link:
                    rc.write(dfile, link["link_dest"], _config={"NoCheckDest": True})
                    m = f"apath = {_r(file['apath'])} is a LINK to {_r(link['link_dest'])}"
                    debug(m)
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
                msg = [f"ERROR: Could not upload {_r(file['apath'])}."]
                msg.append(f"Error: {EE}")
                log("\n".join(msg))
                with LOCK:
                    self.errcount += 1

        files = tmap(partial(_transfer_rc, rc=rc), files, Nt=config.concurrency)
        files = filter(bool, files)
        # We could theoretically do an insert_many but that could lock the DB and/or
        # require we accumulate. Instead, let it go right to insert which closes the DB
        # each time. This is trivial compared to upload times. Plus, we want an upload
        # to be recorded in the DB right away
        #
        # This does nothing in capture mode
        files = map(
            partial(self.dstdb.insert, savelog=self.config.upload_snapshots), files
        )

        stats = StatsThread(self.config, N=N, daemon=True).start()

        # Make them work
        for file in files:
            stats += 1

        stats.join()

        if self.errcount:
            msg = "ERROR: At least one transfer did not work."
            log(msg)
            raise ValueError(msg)

        # For testing only
        if "backup_transfer" in _FAIL:
            raise ValueError()

    def reference(self, shell_script=False):
        config = self.config

        # The upload pipeline is done in a functional programing(esque) fashion
        # to better enable concurrency.
        self.errcount = 0
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

        if shell_script:
            out = []
            cmd = [config.rclone_exe] + config.rclone_flags
            cmd.extend(["rcat", "--no-check-dest"])
            for file in files:
                ref_rpath = file["ref_rpath"]
                rpath = file["rpath"]

                ref = {
                    "ver": 2,
                    "rel": os.path.relpath(rpath, os.path.dirname(ref_rpath)),
                }
                reftxt = json.dumps(ref)

                dst = rcpathjoin(config.dst, ref_rpath)

                echo = shlex.join(["printf", reftxt])
                rcat = shlex.join(cmd + [dst])

                out.append(f"{echo} |  {rcat}")
            return "\n".join(out)

        rc = self.config.rc
        rc.start_rc()

        def _upload_ref(file):
            original = file["original"]
            ref_rpath = file["ref_rpath"]
            rpath = file["rpath"]

            ref = {"ver": 2, "rel": os.path.relpath(rpath, os.path.dirname(ref_rpath))}
            reftxt = json.dumps(ref)
            try:
                log(
                    f"Moving {_r(original)} to {_r(file['apath'])} with {_r(ref_rpath)}."
                )
                rc.write(
                    (config.dst, ref_rpath),
                    reftxt,
                )
                return file
            except Exception as EE:
                msg = [f"ERROR: Could not upload {_r(file['apath'])}."]
                msg.append(f"Error: {EE}")
                log("\n".join(msg))
                with LOCK:
                    self.errcount += 1

        files = tmap(_upload_ref, files, Nt=config.concurrency)
        files = filter(bool, files)
        files = map(
            partial(self.dstdb.insert, savelog=self.config.upload_snapshots), files
        )

        # Make them work
        for file in files:
            pass
        if self.errcount:
            msg = "ERROR: At least one reference (move) did not work."
            log(msg)
            raise ValueError(msg)

        # For testing only
        if "backup_reference" in _FAIL:
            raise ValueError()

    def move_by_copy(self, shell_script=False):
        config = self.config

        # The upload pipeline is done in a functional programing(esque) fashion
        # to better enable concurrency.
        self.errcount = 0
        moves = iter(self.moves)

        def _build_copiedfile(original_dfile, moved_sfile):
            ts = self.config.now.ts

            new = original_dfile.copy()
            new.update(moved_sfile)

            new["original"] = original_dfile["apath"]
            new["rpath"] = apath2rpath(new["apath"], ts)
            new["source_rpath"] = original_dfile["rpath"]
            new["timestamp"] = ts
            new["dstinfo"] = False  # Since this is coming from the source
            return new

        files = map(star(_build_copiedfile), moves)

        if shell_script:
            out = []
            cmd = [config.rclone_exe] + config.rclone_flags
            cmd.extend(["copyto", "--no-check-dest"])
            for file in files:
                sfile = rcpathjoin(self.config.dst, file["source_rpath"])
                dfile = rcpathjoin(self.config.dst, file["rpath"])

                out.append(shlex.join(cmd + [sfile, dfile]))

            return "\n".join(out)

        rc = self.config.rc
        rc.start_rc()

        def _copy_rc(file, *, rc):
            try:
                msg = f'"Moving" {_r(file["original"])} to {_r(file["apath"])} via copy'

                sfile = rcpathjoin(self.config.dst, file["source_rpath"])
                dfile = rcpathjoin(self.config.dst, file["rpath"])

                log(msg)

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
                msg = [f"ERROR: Could not copy {_r(file['apath'])}."]
                msg.append(f"Error: {EE}")
                log("\n".join(msg))
                with LOCK:
                    self.errcount += 1

        files = tmap(partial(_copy_rc, rc=rc), files, Nt=config.concurrency)
        files = filter(bool, files)
        files = map(
            partial(self.dstdb.insert, savelog=self.config.upload_snapshots), files
        )

        # Make them work
        for file in files:
            pass
        if self.errcount:
            msg = "ERROR: At least one move (via copy) did not work."
            log(msg)
            raise ValueError(msg)

        # For testing only
        if "backup_copy" in _FAIL:
            raise ValueError()

    def delete(self, shell_script=False):
        config = self.config
        # dst_rclone = self.config.dst_rclone

        # The upload pipeline is done in a functional programing(esque) fashion
        # to better enable concurrency.
        self.errcount = 0
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

        if shell_script:
            out = []
            cmd = [config.rclone_exe] + config.rclone_flags
            cmd.extend(["rcat", "--no-check-dest"])
            for file in files:
                dst = rcpathjoin(config.dst, file["rpath"])

                echo = shlex.join(["echo", "DEL"])
                rcat = shlex.join(cmd + [dst])

                out.append(f"{echo} |  {rcat}")
            return "\n".join(out)

        rc = self.config.rc
        rc.start_rc()

        def _delete(file):
            dfile = file["rpath"]
            try:
                log(f"Deleting {_r(file['apath'])} with {_r(dfile)}.")
                rc.write(
                    (config.dst, dfile),
                    b"DEL",
                )
                return file
            except subprocess.CalledProcessError as EE:
                log(f"ERROR: Could not upload {_r(file['rpath'])}.")
                log(f"Error: {EE}")
                with LOCK:
                    self.errcount += 1

        files = tmap(_delete, files, Nt=config.concurrency)
        files = filter(bool, files)
        files = map(
            partial(self.dstdb.insert, savelog=self.config.upload_snapshots), files
        )

        # Make them work
        for file in files:
            pass

        if self.errcount:
            msg = "ERROR: At least one delete did not work."
            log(msg)
            raise ValueError(msg)

        # For testing only
        if "backup_delete" in _FAIL:
            raise ValueError()

    def action_summary(self):
        self.action_summary_text = []

        _p = debug
        if self.config.cliconfig.dry_run or self.config.cliconfig.interactive:
            _p = log

        m = f"New: {self.summary(self.new)}"
        self.action_summary_text.append(m)
        log(m)
        for file in self.new:
            _p(f"   {_r(file)}")

        m = f"Modified: {self.summary(self.modified)}"
        self.action_summary_text.append(m)
        log(m)
        for file in self.modified:
            _p(f"   {_r(file)}")

        m = f"Deleted: {self.summary(self.deleted,src=False)}"
        self.action_summary_text.append(m)
        log(m)
        for file in self.deleted:
            _p(f"   {_r(file)}")

        m = f"Moves: {self.summary([f[0]['apath'] for f in self.moves],src=False)}"
        self.action_summary_text.append(m)
        log(m)
        for file in self.moves:
            _p(f"   {_r(file[0]['apath'])} --> {_r(file[1]['apath'])}")

    def summary(self, files, src=True):
        flist = self.src_files if src else self.dst_files
        size = sum(flist[file]["size"] for file in files)
        num, units = bytes2human(size)
        s = "s" if len(files) != 1 else ""
        return f"{len(files)} file{s} ({num:0.2f} {units})"

    def run_stats(self):
        stats = []
        select = """
            SUM(CASE 
                WHEN (size >= 0 AND (isref IS NULL OR isref = 0) )
                THEN size ELSE 0 END) 
            AS totsize,
            COUNT(size) as num
            """
        cur = self.dstdb.snapshot(select=select).fetchone()
        num, units = bytes2human(cur["totsize"])
        s = "s" if cur["num"] != 1 else ""
        stats.append(f"Current {cur['num']} file{s} ({num:0.2f} {units})")

        tot = self.dstdb.db().execute(f"SELECT {select} FROM items").fetchone()
        num, units = bytes2human(tot["totsize"])
        s = "s" if tot["num"] != 1 else ""
        stats.append(f"Total {tot['num']} file{s} ({num:0.2f} {units})")

        stats.extend(self.action_summary_text)
        stats.append(f"Elapsed Time (approx): {time_format(time.time() - self.t0)}")
        return "\n".join(stats)

    def call_shell(self, *, mode, stats=""):
        dry = self.config.cliconfig.dry_run or self.config.cliconfig.shell_script

        if mode == "pre":
            cmds = self.config.pre_shell
        elif mode == "post":
            cmds = self.config.post_shell

        if not cmds:
            debug(f"No cmds for {mode = }")
            return

        env = {"CONFIGDIR": self.config._config["__dir__"], "STATS": stats}

        returncode = shell_runner(cmds, dry=dry, env=env, prefix=f"{mode}.shell")

        if returncode and self.config.stop_on_shell_error:
            raise subprocess.CalledProcessError(returncode, cmds)

    def upload_logs(self):
        config = self.config
        # dst_rclone = self.config.dst_rclone

        if not log.log_file.exists():
            debug("no log file")
            return

        name = f"{self.config.now.dt}Z.log"
        log_dests = [
            rcpathjoin(log_dest, name) for log_dest in listify(config.log_dest)
        ]

        if config.upload_logs:
            log_dests.append((config.dst, f".dfb/logs/{name}"))

        if not log_dests:
            debug("no log destinations")
            return

        # Need to copy the log file since it may change in the process of the upload
        # from the calls itself
        log_copy = log.log_file.with_stem("log_copy")
        shutil.copy2(log.log_file, log_copy)

        for log_dest in log_dests:
            dtxt = rcpathjoin(*listify(log_dest))
            log(f"Uploading log to {_r(dtxt)}")
            try:
                config.rc.copyfile(
                    src=log_copy,
                    dst=log_dest,
                )
            except Exception as e:
                log(f"Failed: {e}")

        if self.config.upload_snapshots:
            name = f"{self.config.now.dt}Z.jsonl"
            snap_src = self.config.tmpdir / name
            snap_dst = (config.dst, f".dfb/snapshots/{name}")
            try:
                config.rc.copyfile(
                    src=snap_src,
                    dst=snap_dst,
                )
            except Exception as e:
                log(f"Failed: {e}")


SHELL_SCRIPT_WARNING = dedent(
    """\
    !!!!!!!!!!!!!!!!!!!!!!!! WARNING !!!!!!!!!!!!!!!!!!!!!!!!
    ╔═══════════════════════════════════════════════════════╗
    ║                                                       ║
    ║  #     #    #    ######  #     # ### #     #  #####   ║
    ║  #  #  #   # #   #     # ##    #  #  ##    # #     #  ║
    ║  #  #  #  #   #  #     # # #   #  #  # #   # #        ║
    ║  #  #  # #     # ######  #  #  #  #  #  #  # #  ####  ║
    ║  #  #  # ####### #   #   #   # #  #  #   # # #     #  ║
    ║  #  #  # #     # #    #  #    ##  #  #    ## #     #  ║
    ║   ## ##  #     # #     # #     # ### #     #  #####   ║
    ║                                                       ║
    ╚═══════════════════════════════════════════════════════╝
    The local database will **NOT** be updated if the shell 
    script is run manually. You MUST run with --refresh
    next time you use dfb!
    !!!!!!!!!!!!!!!!!!!!!!!! WARNING !!!!!!!!!!!!!!!!!!!!!!!!
    """
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
            speednum, speedunits = bytes2human(stats["speed"])
            totnum, totunits = bytes2human(stats["totalBytes"])
            dt = time_format(stats["elapsedTime"])

            msg = [f"STATS: Elapsed {dt};"]
            msg.append(f"Transfering {len(stats.get('transferring',0))};")
            msg.append(f"Avg. Speed {speednum:0.2f} {speedunits}/sec;")
            # stats['totalTransfers'] includes active so use self.fcount

            msg.append(f"Total {self.fcount}/{self.N} ({totnum:0.2f} {totunits})")
            log(" ".join(msg))

    def join(self, *a, **k):
        self.stop.put(True)
        super().join(*a, **k)
        debug("Joined stats thread")
