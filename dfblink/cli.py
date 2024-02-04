import os, sys
import argparse
import logging

__version__ = "20240102.0"

from dfb import __version__ as dfb_version
from dfb.cli import ISODATEHELP

from .link import dfblink

epilog = """\
Note on Mounting
----------------
The 'mount' location can point to a sub directory of the backup but the rclone
mount should be at the highest level. This is to allow for reference files to
correctly resolve.

For example, if you backups to 'backups:' and want to look at the subdirectory
'Documents/Pictures', you should

    $ rclone mount backups: mountpoint [mount flags ...]
    $ %(prog)s mountpoint/Documents/Pictures dest [link flags ...]

so that references point above 'Documents/Pictures' will resolve.

It is also suggested to use mount flags '--read-only' and maybe '--vfs-cache-mode full'
"""

description = """\
Make symlinks to an rclone mount (or local path) of the backups
"""


def cli(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser(
        description=description,
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "mount",
        metavar="mountpoint",
        help="""
            Specify where rclone is mounted including possible subdirectories. 
            See "Note on Mounting" for details on subdirectories
        """,
    )
    parser.add_argument("dest", help="Specify the destination directory for the links")

    parser.add_argument(
        "--before",
        "--at",
        default=None,
        help=f"Specify a timestamp to mimic. Defaults to latest. {ISODATEHELP}",
    )
    parser.add_argument(
        "--after",
        default=None,
        help="""
            Specify the earliest timestamp to include. Defaults to earliest. 
            See --before for details on timestamps
        """,
    )
    parser.add_argument(
        "--max-depth",
        default=None,
        type=int,
        metavar="N",
        help="""
            Specify a maximum depth to descend and create links. 
            Default is no limit. Top level is 0
            """,
    )
    parser.add_argument(
        "--allow-non-empty",
        action="store_true",
        help="Allow 'dest' to not be empty. Otherwise will error",
    )
    parser.add_argument(
        "--force-overwrite",
        action="store_true",
        help="Allow %(prog)s to overwrite existing files in 'dest'. Otherwise, will error if one already exists",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Verbosity. Specify twice for debug mode",
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"dfb-link.{__version__},dfb-{dfb_version}",
    )

    args = parser.parse_args(argv)

    args.verbose = max([0, min([args.verbose, 2])])
    logging.basicConfig(
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=[logging.WARNING, logging.INFO, logging.DEBUG][
            getattr(args, "verbose", 0)
        ],
    )

    logging.logger.debug(" argv: %s", str(argv))
    logging.logger.debug(" args: %s", args)

    try:
        dfblink(
            args.mount,
            args.dest,
            before=args.before,
            after=args.after,
            maxdepth=args.max_depth,
            allow_non_empty=args.allow_non_empty,
            force_overwrite=args.force_overwrite,
        )
    except Exception as E:
        logging.error(str(E))
        if args.verbose >= 2:
            raise
        sys.exit(2)
