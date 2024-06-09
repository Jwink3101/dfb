"""
Restores
"""

import os, sys
import subprocess
import shlex
import logging

from . import LOCK
from .dstdb import DFBDST
from .rclonerc import rcpathjoin, rcpathsplit
from .utils import human_readable_bytes, star, listify, shell_header
from .threadmapper import thread_map_unordered as tmap

logger = logging.getLogger(__name__)


class SourceNotFoundError(ValueError):
    pass


class Restore:
    def __init__(self, config):
        self.config = config
        self.args = args = config.cliconfig

        # Handle @src alone or @src/. But we do not want @srcc to work.
        if args.dest == "@src":
            args.dest = config.src
        elif args.dest.startswith("@src/"):
            args.dest = rcpathjoin(config.src, args.dest[5:])  # 5: for /

        self.dstdb = DFBDST(config)

        if args.command == "restore-file":
            self.restore_file()
        else:
            self.restore_dir()

        self.summary()

        if not self.transfers:
            logger.info("No Transfers")
            return

        if self.args.dry_run:
            logger.info("DRY-RUN. Exit")
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
            path=args.source,
            before=args.before,
            after=args.after,
            select="apath,rpath,size",
        )
        snap = map(self.dstdb.fullrow2dict, snap)

        # transfers: remote-source (rel to remote), final dest
        transfers = (
            (row["rpath"], (args.dest, row["apath"]), row["size"]) for row in snap
        )

        self.transfers = list(transfers)
        if not transfers:
            raise SourceNotFoundError(
                f"Could not find any files at {args.source!r} at the specified time"
            )

    def restore_file(self):
        args = self.args

        row = self.dstdb.snapshot(
            before=args.before,
            after=args.after,
            select="apath,rpath,size",
            conditions=[("apath = :file", {"file": args.source})],
        ).fetchone()

        if not row:
            raise SourceNotFoundError(
                f"Could not find {args.source!r} at the specified time"
            )

        if args.dest == "-":
            dest = "-"
        elif args.to:
            dest = args.dest
        else:
            dest = (args.dest, os.path.basename(args.source))

        self.transfers = [(row["rpath"], dest, row["size"])]

    def summary(self):
        _p = logger.debug
        if self.args.dry_run or self.args.interactive:
            _p = logger.info

        num, units = human_readable_bytes(sum(r[-1] for r in self.transfers))
        s = "s" if len(self.transfers) != 1 else ""
        logger.info(f"Restoring {len(self.transfers)} file{s} ({num:0.2f} {units})")

        for src, dst, size in self.transfers:
            num, units = human_readable_bytes(size)
            s = rcpathjoin(self.config.dst, src)
            d = rcpathjoin(*listify(dst))
            if d == "-":
                d = "<<stdout>>"
            _p(f"    {s!r} --> {d!r} ({num:0.2f} {units})")

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
            print("\n".join(out), flush=True)
        else:
            with open(self.args.shell_script, "wt") as fp:
                fp.write("\n".join(out))
            logger.info(f"Shell script written to {self.args.shell_script!r}")

    def transfer(self):
        config = self.config
        rc = config.rc
        rc.start()

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
                            logger.info(
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
                logger.info(f"Transfering {stxt!r} to {dtxt!r}.")

                meta = self.config.metadata
                if stxt.endswith(".rclonelink"):
                    meta = False

                rc.copyfile(
                    src=src,
                    dst=dst,
                    _config={
                        "NoCheckDest": self.args.no_check,
                        "metadata": meta,
                    },
                )
            except Exception as EE:
                msg = [f"ERROR: Could not restore {src0!r}."]
                msg.append(f"Error: {EE}")
                logger.error("\n".join(msg))
                with LOCK:
                    self.errcount += 1

        transfers = iter(self.transfers)
        transfers = (t[:2] for t in transfers)
        transfers = tmap(star(_transfer_rc), transfers, Nt=config.concurrency)
        for _ in transfers:
            pass

        if self.errcount:
            msg = "ERROR: At least one restore did not work."
            logger.info(msg)
            raise ValueError(msg)
