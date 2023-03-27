#!/usr/bin/env python
import sys

# This shouldn't be needed since I have python_requires set but just in case:
if sys.version_info < (3, 9):
    raise ValueError("Must use python >= 3.9")

import dfb

dfb_version = dfb.__version__

from setuptools import setup

# This will set the __git_version__ in __init__.py if possible then change it back after
# install.
import subprocess, os, re

pwd = os.path.abspath(os.path.dirname(__file__))
try:
    version = (
        subprocess.check_output(["git", "log", "-1", "--format=%h"], cwd=pwd)
        .decode("utf8")
        .strip()
    )
    dfb_version += f".{version}"

    origin = subprocess.check_output(["git", "remote", "-v"], cwd=pwd).decode()
    origin = re.search(r"origin\s+?(\S.*?)\s+?\(fetch\)", origin)
    if origin:
        origin = origin.group(1)

    git = {"version": version, "origin": origin}

except:
    git = None

try:
    try:
        stat = os.stat("dfb/__init__.py")
        with open("dfb/__init__.py") as fp:
            init0 = init = fp.read()
        init = init.replace("__git_version__ = None", f"__git_version__ = {repr(git)}")
        with open("dfb/__init__.py", "wt") as fp:
            fp.write(init)
    except:
        init0 = None

    setup(
        name="dfb",
        packages=["dfb"],
        long_description=open("readme.md").read(),
        install_requires=["requests"],
        entry_points={
            "console_scripts": ["dfb=dfb.cli:cli", "dfbshebanged=dfb.cli:clishebang"],
        },
        version=dfb_version,
        description="Dated File Backup",
        url="https://github.com/Jwink3101/dfb/",
        author="Justin Winokur",
        author_email="Jwink3101@users.noreply.github.com",
        license="MIT",
        python_requires=">=3.9",
    )
finally:
    if init0:
        with open("dfb/__init__.py", "wt") as fp:
            fp.write(init0)
        os.utime("dfb/__init__.py", (stat.st_atime, stat.st_mtime))
