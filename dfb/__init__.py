import datetime
import logging
import os

from .configuration import LOCK
from .utils import time2all, rpath2apath, apath2rpath
from .timestamps import timestamp_parser

logger = logging.getLogger(__name__)

__version__ = "BETA.20241231.0"
MIN_RCLONE = 1, 63, 0


# This is a bit of a hack. When setup.py is run, if and only if, it is a git repo, this
# will get set. Otherwise, it is None
__git_version__ = None

_override_ts = os.environ.get("DFB_OVERRIDE_TIMESTAMP", None)
_override_unix = os.environ.get("DFB_OVERRIDE_UNIXTIME", None)
_override_offset = 0


def nowfun():
    """
    Return current datetime.
    This is its own function so _override_unix and _override_offset can
    be set in tests
    """
    if _override_ts:
        now = timestamp_parser(_override_ts)
    elif _override_unix:
        now = datetime.datetime.fromtimestamp(_override_unix, datetime.timezone.utc)
    else:
        now = datetime.datetime.now()

    if _override_offset:
        now += datetime.timedelta(seconds=_override_offset)

    return time2all(now)


def nowfun_obj():
    return nowfun().obj


# Monkey patch this into timestamps. We do this so that timestamps *can* exists 100%
# without dfb (such as if I ever want to copy/paste the code) but for dfb, we want this
# version of nowfun since it can be overridden for testing
from . import timestamps

timestamps.nowfun = nowfun

# For testing only. I can add fail points
_FAIL = set()
