import datetime
import string
import re

from . import debug, log

_r = repr


def timestamp_parser(timestamp, aware=False, utc=False, epoch=False, now=None):
    """
    Will either accept iso8601 and pass aware,utc, and epoc
    or will take a string that contains one or more of
        ["seconds","minutes","hours","days","weeks"]
    and be a differences from "now" if specified else will use current time

    """
    delta = timedelta_parser(timestamp)
    if delta:
        if not now:
            from . import nowfun

            now = nowfun().obj
        timestamp = timestamp_parser(now, aware=aware, utc=utc) - delta

    return iso8601_parser(timestamp, aware=aware, utc=utc, epoch=epoch)


def timedelta_parser(deltastr):
    """
    Return a timedelta object or None
    """
    if not isinstance(deltastr, str):
        return

    deltastr0 = deltastr
    deltastr = deltastr.lower().replace(",", " ")

    keys = ["seconds", "minutes", "hours", "days", "weeks"]

    delta = {}
    for key in keys:
        if key[:-1] not in deltastr:
            continue  # the [:-1] removes an "s"

        val = re.search(
            r"([\d|\.]+)\ *?KEY?".replace("KEY", key), deltastr, flags=re.IGNORECASE
        )
        if val:
            delta[key] = float(val.group(1))
    if delta:
        try:  # not worth failing over
            debug(f"Processed {_r(deltastr0)} to {delta}")
        except:
            pass
        return datetime.timedelta(**delta)
    else:
        return


def iso8601_parser(timestamp, aware=False, utc=False, epoch=False):
    """
    This will parse an ISO-8601 style datetimes including RFC 3339. It is designed to
    parse different variants of YYYY-MM-DD HH:MM:SS, optionally with time zones and
    precision. While it will accept nanosecond precision, it will not capture beyond
    microseconds due to Python's limitations.

    Alternatively, if timestamp is an integer or "i<integer>", it will be considered
    the UNIX Epoch time.

    Only accepts time zone offsets, not names. Years MUST be four digits. Can also accept
    just date specification but still must be four digit years and can accept times
    without minutes or seconds

    Inputs:
    -------
    timestamp
        Timestamp to parse. See above for requirements

    aware [False]
        If True, will assume timestamps without specified timezones are in
        local time. If set to 'utc', will assume the unsepcified time zone IS utc time.

        If a timezone is in the timestamp, this will have no effect

    utc [False]
        Convert all responses to UTC time regardless of specified times. If the
        timestamp doesn't have a timezone, it assumed local unless `aware = 'utc'`

     epoch [False]
         Return epoch time instead of a datetime object

    Returns:
    --------
    datetime object or epoch float
    """
    timestamp0 = timestamp
    if isinstance(timestamp, str) and (
        timestamp.startswith("i") or timestamp.startswith("u")
    ):
        timestamp = float(timestamp[1:])  # May have to deal with 2038 problem?
    if isinstance(timestamp, (int, float)):
        timestamp = datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)

    # This is the main formatting. If not a datetime, it is a recursive call to this
    # part below.
    if isinstance(timestamp, datetime.datetime):
        dt = timestamp

        # https://stackoverflow.com/a/27596917 and https://docs.python.org/3/library/datetime.html#determining-if-an-object-is-aware-or-naive
        isaware = dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None
        if (aware or utc) and not isaware:
            if aware == "utc":
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            else:  # local
                # I am not sure why .astimezone() isn't working as one would expect with
                # DST. See https://www.reddit.com/r/learnpython/comments/1214sa1/different_timezones_from_astimezone_parsed_naive/
                # where I asked

                # What *should* work
                # dt = dt.astimezone()

                # Alternative
                tz = datetime.datetime.now().astimezone().tzinfo
                dt = dt.replace(tzinfo=tz)

        if utc:
            dt = dt.astimezone(datetime.timezone.utc)

        if epoch:
            return datetime.datetime.timestamp(dt)
        return dt

    timestamp = timestamp.strip()
    if not timestamp:
        return

    # Handle no time specified but still must be four digit year
    n = sum(c in string.digits for c in timestamp)
    if n <= 6:  # This won't catch them all but still will catch some.
        raise ValueError(
            "MUST at least a FOUR digit year, two digit month, and two digit day. "
            f"Specified: {_r(timestamp0)}"
        )
    if n == 8:
        timestamp = f"{timestamp} 00:00:00"

    timestamp = timestamp.lower().replace(":", "").replace("t", "")

    # pull timezone
    if timestamp.endswith("z"):
        tz = "+0000"
        timestamp = timestamp[:-1]
    elif timestamp[-5] in "-+":
        tz = timestamp[-5:]
        timestamp = timestamp[:-5]
    elif timestamp[-3] in "-+":
        tz = timestamp[-3:] + "00"
        timestamp = timestamp[:-3]
    else:
        tz = None

    # Now get rid of anything that isn't numeric or dot
    timestamp = "".join(c for c in timestamp if c in string.digits + ".")

    timestamp = timestamp.split(".")
    if len(timestamp) == 1:  # No precision
        timestamp = timestamp[0].ljust(14, "0")  # Pad if not given with MM or SS
        us = ""
    else:
        us = timestamp[-1]
        timestamp = "".join(timestamp[:-1])  # also remove '.'

    # Force microseconds so that we are consistent
    us = round(float(f".{us}000000") * 1e6)
    timestamp = f"{timestamp}.{us:06d}"

    if tz:  # Already will be aware
        res = datetime.datetime.strptime(timestamp + tz, "%Y%m%d%H%M%S.%f%z")
    else:
        res = datetime.datetime.strptime(timestamp, "%Y%m%d%H%M%S.%f")

    # Recursive call to the top for formatting
    return timestamp_parser(res, aware=aware, utc=utc, epoch=epoch)
