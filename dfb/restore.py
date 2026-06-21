"""
Restores
"""

import datetime
import logging
import os
import subprocess
import sys
from types import SimpleNamespace

from . import LOCK, MIN_RCLONE
from .dstdb import DFBDST
from .rclonerc import rcpathjoin
from .threadmapper import thread_map_unordered as tmap
from .utils import human_readable_bytes, listify, star

logger = logging.getLogger(__name__)


class SourceNotFoundError(ValueError):
    pass


class Restore:
    def __init__(self, config):
        self.config = config
        self.args = args = config.cliconfig
        self._restore_metadata_logged = False
        self._restore_metadata_state = SimpleNamespace(
            enabled=bool(self.config.restore_metadata_source),
            live_mode=self.config.restore_metadata_source_live,
            source_slow_modtime=None,
        )

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

        self._preload_transfer_rows()
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

        self.transfer()

    def restore_dir(self):
        args = self.args

        snap = self.dstdb.snapshot(
            path=args.source,
            before=args.before,
            after=args.after,
            select="apath,rpath,size,timestamp",
        )
        snap = map(self.dstdb.fullrow2dict, snap)

        # transfers: remote-source (rel to remote), final dest
        transfers = (
            (
                row["rpath"],
                (args.dest, row["apath"]),
                row["size"],
                row["apath"],
                row["timestamp"],
            )
            for row in snap
        )

        self.transfers = list(transfers)
        if not self.transfers:
            raise SourceNotFoundError(
                f"Could not find any files at {args.source!r} at the specified time"
            )

    def restore_file(self):
        args = self.args

        row = self.dstdb.snapshot(
            before=args.before,
            after=args.after,
            select="apath,rpath,size,timestamp",
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

        self.transfers = [
            (row["rpath"], dest, row["size"], row["apath"], row["timestamp"])
        ]

    def _preload_transfer_rows(self):
        cached = {}
        transfers = []

        with self.dstdb.db() as db:
            for src, dst, size, apath, timestamp in self.transfers:
                cache_key = (apath, timestamp)
                if cache_key not in cached:
                    row = db.execute(
                        """
                        SELECT * FROM items
                        WHERE apath = ? AND timestamp = ?
                        """,
                        cache_key,
                    ).fetchone()
                    if row:
                        row = self.dstdb.fullrow2dict(row)
                    else:
                        logger.warning(
                            "Could not load full restore metadata row for %r at %s. "
                            "Will restore without DB-backed metadata replay.",
                            apath,
                            timestamp,
                        )
                    cached[cache_key] = row
                transfers.append((src, dst, size, cached[cache_key]))

        self.transfers = transfers

    def _check_rclone_version(self):
        ver = self.config.rc.call("core/version")
        logger.info("rclone version: " + ".".join(str(i) for i in ver["decomposed"]))
        logger.debug("rclone version details: %r", ver)

        decomposed = tuple(int(c) for c in ver["decomposed"])
        if decomposed < MIN_RCLONE:
            raise ValueError(
                "Unsupported rclone version. "
                f"Must use {'.'.join(f'{i}' for i in MIN_RCLONE)} or newer"
            )

    def _log_restore_metadata_state(self):
        if self._restore_metadata_logged:
            return

        state = self._restore_metadata_state
        if not state.enabled:
            self._restore_metadata_logged = True
            return

        if state.live_mode == "auto":
            state.source_slow_modtime = bool(
                self.config.rc.features(self.config.dst)
                .get("Features", {})
                .get("SlowModTime", False)
            )
            resolution = "disable" if state.source_slow_modtime else "keep"
            logger.debug(
                "Restore DB-backed metadata replay: enabled. "
                "Live source metadata policy=auto; will %s live source metadata "
                "when replay is active (restore source SlowModTime=%s).",
                resolution,
                state.source_slow_modtime,
            )
        else:
            logger.debug(
                "Restore DB-backed metadata replay: enabled. "
                "Live source metadata policy=%s.",
                state.live_mode,
            )

        self._restore_metadata_logged = True

    def _default_live_metadata(self, src):
        if src.endswith(".rclonelink"):
            return False
        return self.config.metadata

    def _format_restore_mtime(self, mtime):
        try:
            dt = datetime.datetime.fromtimestamp(float(mtime), datetime.timezone.utc)
        except (TypeError, ValueError, OverflowError):
            logger.debug("Skipping restore mtime metadata value %r.", mtime)
            return None
        return dt.isoformat().replace("+00:00", "Z")

    def _metadata_candidates_from_row(self, row):
        metadata_set = {}
        nested_metadata = row.get("Metadata", None)

        if isinstance(nested_metadata, dict):
            metadata_set.update(nested_metadata)
        elif nested_metadata is not None:
            logger.debug(
                "Skipping DB-backed restore metadata container for %r because "
                "'Metadata' is not a dictionary.",
                row.get("rpath"),
            )

        if (mtime := row.get("mtime", None)) is not None:
            if mtime := self._format_restore_mtime(mtime):
                metadata_set["mtime"] = mtime

        return metadata_set

    def _restore_metadata_config(self, src, dst, row):
        config = {"Metadata": self._default_live_metadata(src)}
        state = self._restore_metadata_state

        if (
            not state.enabled
            or dst == "-"
            or src.endswith(".rclonelink")
            or row is None
            or row.get("dstinfo", True)
        ):
            return config

        metadata_set = self._metadata_candidates_from_row(row)
        if not metadata_set:
            return config

        # rclone only applies MetadataSet when metadata mode is enabled.
        config["Metadata"] = True
        config["MetadataSet"] = metadata_set

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "DB-backed restore metadata replay for %r using keys: %s",
                row.get("rpath"),
                ", ".join(sorted(metadata_set)),
            )

        return config

    def summary(self):
        _p = logger.debug
        if self.args.dry_run or self.args.interactive:
            _p = logger.info

        num, units = human_readable_bytes(sum(r[2] for r in self.transfers))
        s = "s" if len(self.transfers) != 1 else ""
        logger.info(f"Restoring {len(self.transfers)} file{s} ({num:0.2f} {units})")

        for src, dst, size, _ in self.transfers:
            num, units = human_readable_bytes(size)
            s = rcpathjoin(self.config.dst, src)
            d = rcpathjoin(*listify(dst))
            if d == "-":
                d = "<<stdout>>"
            _p(f"    {s!r} --> {d!r} ({num:0.2f} {units})")

    def transfer(self):
        config = self.config
        rc = config.rc
        rc.start()
        self._check_rclone_version()
        self._log_restore_metadata_state()

        self.errcount = 0

        def _transfer_rc(src, dst, row):
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

                copy_config = {"NoCheckDest": self.args.no_check}
                copy_config |= self._restore_metadata_config(src0, dst, row)

                rc.copyfile(
                    src=src,
                    dst=dst,
                    _config=copy_config,
                )
            except Exception as EE:
                msg = [f"ERROR: Could not restore {src0!r}."]
                msg.append(f"Error: {EE}")
                logger.error("\n".join(msg))
                with LOCK:
                    self.errcount += 1

        transfers = iter(self.transfers)
        transfers = ((src, dst, row) for src, dst, _size, row in transfers)
        transfers = tmap(star(_transfer_rc), transfers, Nt=config.concurrency)
        for _ in transfers:
            pass

        if self.errcount:
            msg = "ERROR: At least one restore did not work."
            logger.info(msg)
            raise ValueError(msg)
