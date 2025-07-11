#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

p = os.path.abspath("../")
if p not in sys.path:
    sys.path.insert(0, p)

from dfb.timestamps import timestamp_parser


def test_timestamp_parser():
    # These were generated at about the same time but may be *slightly* different
    nominally_same = [
        "2023-03-25T00:06:27.123456789+00:00",
        "2023-03-25T00:06:27.123456+00:00",
        "2023-03-25T00:06:27.123+00:00",
        "2023-03-25T00:06:27+00:00",
        "2023-03-24T18:06:27.123456789-06:00",
        "2023-03-24T18:06:27.123456-06:00",
        "2023-03-24T18:06:27.123-06:00",
        "2023-03-24T18:06:27-06:00",
        "2023-03-24T18:06:27.123456",
        "2023-03-25T00:06:27Z",
        "2023-03-24T18:06:27.123456",
        "2023-03-24T18:06:27.123",
        "2023-03-24T18:06:27",
        "20230325T00:06:27.123456+00:00",
        "20230325T00:06:27+00:00",
        "20230324T18:06:27.123456-06:00",
        "20230324T18:06:27-06:00",
        "20230325T00:06:27.123456789Z",
        "20230325T00:06:27.123456Z",
        "20230325T00:06:27Z",
        "20230324T18:06:27.123456",
        "20230324T18:06:27",
        "2023-03-25T000627.123456+0000",
        "2023-03-25T000627+0000",
        "2023-03-24T180627.123456789-0600",
        "2023-03-24T180627.123456-0600",
        "2023-03-24T180627-0600",
        "2023-03-25T000627.123456Z",
        "2023-03-25T000627Z",
        "2023-03-24T180627.123456",
        "2023-03-24T180627",
        "20230325T000627.123456+0000",
        "20230325T000627+0000",
        "20230324T180627.123456-0600",
        "20230324T180627-0600",
        "20230325T000627.123456Z",
        "20230325T000627Z",
        "20230324T180627.123456",
        "20230324T180627",
    ]
    for t in nominally_same[:]:
        nominally_same.append(t.replace("T", " "))

    t0 = timestamp_parser(nominally_same[0], epoch=True)
    for t in nominally_same:
        assert abs(timestamp_parser(t, epoch=True) - t0) < 1

        # Test different configs
        timestamp_parser(t, aware=False, utc=False, epoch=False)
        timestamp_parser(t, aware=True, utc=False, epoch=False)
        timestamp_parser(t, aware=False, utc=True, epoch=False)
        timestamp_parser(t, aware=False, utc=False, epoch=True)
        timestamp_parser(t, aware=True, utc=True, epoch=False)
        timestamp_parser(t, aware=True, utc=False, epoch=True)
        timestamp_parser(t, aware=True, utc=True, epoch=True)

    # epoch
    assert isinstance(
        timestamp_parser("20230324T180627-0600", epoch=True), (int, float)
    )

    # Test round trips with float and u<float> string
    t = "20230325T000627.692449Z"
    assert timestamp_parser(timestamp_parser(t, epoch=True)) == timestamp_parser(
        t, utc=True
    )
    assert timestamp_parser(f"u{timestamp_parser(t,epoch=True)}") == timestamp_parser(
        t, utc=True
    )
    assert timestamp_parser(f"i{timestamp_parser(t,epoch=True)}") == timestamp_parser(
        t, utc=True
    )

    # Test the different settings
    timestamp_parser(
        "2000-01-02T03:04:05.06"
    ).isoformat() == "2000-01-02T03:04:05.060000"
    # Should assume local time.
    # This one needs to be more manual since it will depend on the current timezone. If tested in UTC, we won't know.
    # But when tested, this works. However, this needed to be modified a bit. See
    # https://www.reddit.com/r/learnpython/comments/1214sa1/different_timezones_from_astimezone_parsed_naive/
    timestamp_parser(
        "2000-01-02T03:04:05.06", aware=True
    ).isoformat()  # == ''2000-01-02T03:04:05.060000-06:00'' (as of testing)

    # Should assume UTC
    assert (
        timestamp_parser("2000-01-02T03:04:05.06", aware="utc").isoformat()
        == "2000-01-02T03:04:05.060000+00:00"
    )

    # Differences with text
    now = "2000-01-02T03:04:05.060000-07:00"  # this is isoformat()ed
    assert (
        timestamp_parser("1 second", now=now).isoformat()
        == "2000-01-02T03:04:04.060000-07:00"
    )
    assert (
        timestamp_parser("1 seconds", now=now).isoformat()
        == "2000-01-02T03:04:04.060000-07:00"
    )
    assert (
        timestamp_parser("2 seconds 1 day 4 minutes 2 hours", now=now).isoformat()
        == "2000-01-01T01:00:03.060000-07:00"
    )
    assert (
        timestamp_parser(
            "1.2 seconds, 3 minutes 4 hours 5 days 6 weeks", now=now
        ).isoformat()
        == "1999-11-15T23:01:03.860000-07:00"
    )
    timestamp_parser(
        "1.2 seconds, 3 minutes 4 hours 5 days 6 weeks", now=now, utc=True
    ).isoformat() == "1999-11-16T06:01:03.860000+00:00"


if __name__ == "__main__":
    test_timestamp_parser()

    print("=" * 50)
    print(" All Passed ".center(50, "="))
    print("=" * 50)
