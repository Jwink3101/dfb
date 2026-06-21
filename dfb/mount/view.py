import json
import logging
import os
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache

from .. import rpath2apath
from ..dstdb import NoTimestampInNameError

logger = logging.getLogger(__name__)

_EMPTY_MARKER_PREFIX = ".dfbempty."
_DEL_SUFFIX = " [DEL]"
_NODATE_SUFFIX = " [NODATE]"


@dataclass
class DirScanResult:
    files: dict
    subdirs: list


class DFBMountView:
    def __init__(
        self,
        root,
        *,
        ts=None,
        after=None,
        remove_empty=False,
        empty_check_depth=None,
        empty_check_fail="hide",
        use_cache=False,
        cache_reset_min=None,
        show_deleted=False,
        undated_files="tag",
    ):
        self.root = os.path.realpath(root)
        self.ts = ts
        self.after = after
        self.remove_empty = remove_empty
        self.empty_check_depth = empty_check_depth
        self.empty_check_fail = empty_check_fail
        self.show_deleted = show_deleted
        self.undated_files = undated_files
        self._lock = threading.Lock()

        self._scan_dir = self._scan_dir_uncached
        self._dir_has_entries = self._dir_has_entries_uncached

        if use_cache:
            self._scan_dir = lru_cache(maxsize=256)(self._scan_dir_uncached)
            self._dir_has_entries = lru_cache(maxsize=512)(
                self._dir_has_entries_uncached
            )
            if cache_reset_min:
                threading.Thread(
                    target=self._reset_cache,
                    args=(cache_reset_min,),
                    daemon=True,
                ).start()

    def _reset_cache(self, cache_reset_min):
        while True:
            time.sleep(cache_reset_min * 60)
            with self._lock:
                if hasattr(self._scan_dir, "cache_clear"):
                    self._scan_dir.cache_clear()
                if hasattr(self._dir_has_entries, "cache_clear"):
                    self._dir_has_entries.cache_clear()

    def _normalize_apath(self, apath):
        if not apath:
            return os.sep
        apath = os.path.normpath(apath)
        if not apath.startswith(os.sep):
            apath = os.sep + apath
        return apath

    def _real_path(self, apath):
        apath = self._normalize_apath(apath)
        return os.path.join(self.root, apath.lstrip(os.sep))

    def _virtual_path(self, rpath):
        rel = os.path.relpath(rpath, self.root)
        if rel.startswith(".."):
            return rpath
        return os.path.join(os.sep, rel)

    def _safe_rpath2apath(self, rpath):
        try:
            return rpath2apath(rpath)
        except (NoTimestampInNameError, ValueError):
            return rpath, None, ""

    def _resolve_reference(self, ref_path):
        try:
            with open(ref_path, "rt") as fp:
                payload = fp.read().strip()
        except OSError as exc:
            logger.debug("Failed to read reference %s: %s", ref_path, exc)
            return None

        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            data = {"ver": 1, "path": payload}

        ver = data.get("ver", 1)
        if ver == 1:
            return data.get("path")
        if ver == 2:
            rel = data.get("rel")
            if not rel:
                return None
            return os.path.normpath(os.path.join(os.path.dirname(ref_path), rel))
        return None

    def _scan_dir_uncached(self, apath_dir):
        apath_dir = self._normalize_apath(apath_dir)
        rdir = self._real_path(apath_dir)
        logger.debug("Scanning directory %s", rdir)

        try:
            entries = list(os.scandir(rdir))
        except FileNotFoundError:
            raise
        except NotADirectoryError:
            raise

        files = []
        subdirs = []
        for entry in entries:
            if entry.is_dir(follow_symlinks=False):
                subdirs.append(entry.name)
            else:
                files.append(entry)

        items = defaultdict(list)
        for entry in files:
            if entry.name.startswith(_EMPTY_MARKER_PREFIX):
                continue

            rpath = entry.path
            apath, ts, flag = self._safe_rpath2apath(rpath)
            vapath = self._virtual_path(apath)

            if self.ts is not None and ts is not None and ts > self.ts:
                continue
            if self.after is not None and ts is not None and ts < self.after:
                continue
            if ts is None and self.undated_files == "hide":
                continue

            if flag == "R":
                referent = self._resolve_reference(rpath)
                if referent:
                    rpath = referent
                    flag = ""

            is_undated = ts is None
            sort_ts = ts if ts is not None else float("-inf")
            items[vapath].append((sort_ts, rpath, flag, is_undated))

        selected = {}
        for vapath, versions in items.items():
            versions.sort(key=lambda v: v[0])
            _, rpath, flag, is_undated = versions[-1]
            if flag == "D":
                if self.show_deleted:
                    selected[f"{vapath}{_DEL_SUFFIX}"] = rpath
                continue
            if self.undated_files == "tag" and is_undated:
                selected[f"{vapath}{_NODATE_SUFFIX}"] = rpath
                continue
            selected[vapath] = rpath

        return DirScanResult(files=selected, subdirs=subdirs)

    def _dir_has_entries_uncached(self, apath_dir, depth_remaining):
        result = self._scan_dir(apath_dir)
        if result.files:
            return True
        if depth_remaining is not None and depth_remaining <= 0:
            if result.subdirs:
                return None
            return False
        next_depth = None if depth_remaining is None else depth_remaining - 1
        unknown = False
        for subdir in result.subdirs:
            sub_apath = os.path.join(self._normalize_apath(apath_dir), subdir)
            has_entries = self._dir_has_entries(sub_apath, next_depth)
            if has_entries is True:
                return True
            if has_entries is None:
                unknown = True
        if unknown:
            return None
        return False

    def listdir(self, apath_dir, *, return_map=False):
        apath_dir = self._normalize_apath(apath_dir)
        result = self._scan_dir(apath_dir)
        entries = dict(result.files)

        for subdir in result.subdirs:
            sub_apath = os.path.join(apath_dir, subdir)
            if self.remove_empty:
                has_entries = self._dir_has_entries(sub_apath, self.empty_check_depth)
                if has_entries is False:
                    continue
                if has_entries is None and self.empty_check_fail == "hide":
                    continue
            entries[sub_apath] = self._real_path(sub_apath)

        if return_map:
            return entries
        return list(entries)

    def apath2rpath(self, apath):
        apath = self._normalize_apath(apath)
        if apath == os.sep:
            return self.root
        parent = os.path.dirname(apath)
        items = self.listdir(parent, return_map=True)
        if apath in items:
            logger.debug("Resolved %s -> %s", apath, items[apath])
            return items[apath]
        return self._real_path(apath)

    def is_deleted_view(self, apath):
        return self.show_deleted and apath.endswith(_DEL_SUFFIX)

    def delete_suffix(self):
        return _DEL_SUFFIX
