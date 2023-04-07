import datetime

from .configuration import log, debug, LOCK
from .utils import time2all
from .timestamps import timestamp_parser

__version__ = "20230407.0"

# This is a bit of a hack. When setup.py is run, if and only if, it is a git repo, this
# will get set. Otherwise, it is None
__git_version__ = None

_override_ts = None
_override_unix = None
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


# For testing only. I can add fail points
_FAIL = set()
