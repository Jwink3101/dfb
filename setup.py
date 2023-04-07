#!/usr/bin/env python
import sys, shutil
import subprocess, os, re

# This shouldn't be needed since I have python_requires set but just in case:
if sys.version_info < (3, 9):
    raise ValueError("Must use python >= 3.9")
_r = repr
import dfb

dfb_version = dfb.__version__

from setuptools import setup

# This will set the __git_version__ in __init__.py if possible then change it back after
# install.


pwd = os.path.abspath(os.path.dirname(__file__))
try:
    version = (
        subprocess.check_output(["git", "log", "-1", "--format=%h"], cwd=pwd)
        .decode("utf8")
        .strip()
    )

    origin = subprocess.check_output(["git", "remote", "-v"], cwd=pwd).decode()
    origin = re.search(r"origin\s+?(\S.*?)\s+?\(fetch\)", origin)
    if origin:
        origin = origin.group(1)

    git = {"version": version, "origin": origin}

except:
    git = None

try:
    shutil.move("dfb/__init__.py", "_tmp.py")
    with open("_tmp.py") as fp:
        init = fp.read()
    init = init.replace("__git_version__ = None", f"__git_version__ = {_r(git)}")
    with open("dfb/__init__.py", "wt") as fp:
        fp.write(init)

    setup(
        name="dfb",
        packages=["dfb", "dfbmount"],
        long_description=open("readme.md").read(),
        install_requires=["requests"],
        entry_points={
            "console_scripts": [
                "dfb=dfb.cli:cli",
                "dfbshebanged=dfb.cli:clishebang",
                "dfb-mount=dfbmount.mount:cli",
            ],
        },
        version=dfb.__version__,  # Do not include git as per PEP440
        description="Dated File Backup",
        url="https://github.com/Jwink3101/dfb/",
        author="Justin Winokur",
        author_email="Jwink3101@users.noreply.github.com",
        license="MIT",
        python_requires=">=3.9",
    )
finally:
    shutil.move("_tmp.py", "dfb/__init__.py")
