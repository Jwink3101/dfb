"""
Restores
"""
import os, sys
import subprocess
import shlex
from . import log, debug, LOCK
from .dstdb import DFBDST
from .rclone import rcpathjoin, rcpathsplit
from .utils import bytes2human, star, listify, shell_header
from .threadmapper import thread_map_unordered as tmap


class SourceNotFoundError(ValueError):
    pass


class Restore:
    def __init__(self, config):
        self.config = config
        self.args = args = config.cliconfig

        # Handle @src
        if args.dest.startswith("@src"):
            args.dest = rcpathjoin(config.src, args.dest[5:])  # 5: for /

        self.dstdb = DFBDST(config)

        if args.command == "restore-file":
            self.restore_file()
        else:
            self.restore_dir()

        self.summary()

        if not self.transfers:
            log("No Transfers")
            return

        if self.args.dry_run:
            log("DRY-RUN. Exit")
            return
        elif self.args.interactive:
            r = input("Do you want to continue? [Y]/N:")
            if r.lower().startswith("n"):
                return

        if self.args.shell_script:
            self.transfer_shell()
        else:
            self.transfer()

    def restore_dir(self):
        args = self.args

        snap = self.dstdb.snapshot(
            path=args.source, before=args.at, select="apath,rpath,size"
        )
        snap = map(self.dstdb.fullrow2dict, snap)

        # transfers: remote-source (rel to remote), final dest
        transfers = (
            (row["rpath"], (args.dest, row["apath"]), row["size"]) for row in snap
        )

        self.transfers = list(transfers)
        if not transfers:
            raise SourceNotFoundError(
                f"Could not find any files at {repr(args.source)} at the specified time"
            )

    def restore_file(self):
        args = self.args

        row = self.dstdb.snapshot(
            before=args.at,
            select="apath,rpath,size",
            conditions=[("apath = ?", args.source)],
        ).fetchone()

        if not row:
            raise SourceNotFoundError(
                f"Could not find {repr(args.source)} at the specified time"
            )

        if args.dest == "-":
            dest = "-"
        elif args.to:
            dest = args.dest
        else:
            dest = (args.dest, os.path.basename(args.source))

        self.transfers = [(row["rpath"], dest, row["size"])]

    def summary(self):
        _p = debug
        if self.args.dry_run or self.args.interactive:
            _p = log

        num, units = bytes2human(sum(r[-1] for r in self.transfers))
        s = "s" if len(self.transfers) != 1 else ""
        log(f"Restoring {len(self.transfers)} file{s} ({num:0.2f} {units})")

        for src, dst, size in self.transfers:
            num, units = bytes2human(size)
            _p(f"    {repr(src)} --> {repr(dst)} ({num:0.2f} {units})")

    def transfer_shell(self):
        config = self.config
        out = [shell_header(config, cd=True)]

        cmd = [config.rclone_exe] + config.rclone_flags
        for src, dst, _ in self.transfers:
            src = rcpathjoin(self.config.dst, src)
            if dst == "-":
                nc = cmd + ["cat", rcpathjoin(self.config.dst, src)]
                out.append(shlex.join(nc))
                continue
            dst = rcpathjoin(*listify(dst))
            nc = cmd + ["copyto", src, dst]
            out.append(shlex.join(nc))

        if self.args.shell_script == "-":
            log.print("\n".join(out), flush=True)
        else:
            with open(self.args.shell_script, "wt") as fp:
                fp.write("\n".join(out))
            log(f"Shell script written to {repr(self.args.shell_script)}")

    def transfer(self):
        config = self.config
        rc = config.rc
        rc.start_rc()

        self.errcount = 0

        def _transfer_rc(src, dst):
            src0 = src
            try:
                src = self.config.dst, src  # ...confusing but should be the dest
                if dst == "-":
                    res = rc.read(src)
                    with LOCK:
                        try:
                            sys.stdout.buffer.write(res + b"\n")
                            sys.stdout.buffer.flush()
                        except AttributeError:
                            log(
                                (
                                    "WARNING: Could not write to stdout buffer. "
                                    "Will try to decode. Otherwise, you should "
                                    "download to file"
                                ),
                                verbosity=0,
                            )
                            sys.stdout.write(res.decode() + "\n")
                            sys.stdout.flush()
                    return
                stxt = rcpathjoin(*listify(src))
                dtxt = rcpathjoin(*listify(dst))
                log(f"Transfering {repr(dtxt)} to {repr(stxt)}.")
                rc.copyfile(
                    src=src,
                    dst=dst,
                    _config={
                        "NoCheckDest": self.args.no_check,
                        "metadata": self.config.metadata,
                    },
                )
            except Exception as EE:
                msg = [f"ERROR: Could not restore {repr(src0)}."]
                msg.append(f"Error: {EE}")
                log("\n".join(msg))
                with LOCK:
                    self.errcount += 1

        transfers = iter(self.transfers)
        transfers = (t[:2] for t in transfers)
        transfers = tmap(star(_transfer_rc), transfers, Nt=config.concurrency)
        for _ in transfers:
            pass

        if self.errcount:
            msg = "ERROR: At least one restore did not work."
            log(msg)
            raise ValueError(msg)


class _do_nothing:
    def __init__(self, *_):
        pass

    def __enter__(self):
        pass

    def __exit__(self, *_):
        pass
