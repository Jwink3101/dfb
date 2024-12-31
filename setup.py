#!/usr/bin/env python
import sys, shutil
import subprocess, os, re
from pathlib import Path

# Read the contents of your README file
this_directory = Path(__file__).parent
long_description = (this_directory / "readme.md").read_text()

# This shouldn't be needed since I have python_requires set but just in case:
if sys.version_info < (3, 9):
    raise ValueError("Must use python >= 3.9")
_r = repr
import dfb


# Extract the version from the module file
def get_version():
    version_file = this_directory / "dfb" / "__init__.py"
    with open(version_file, "r") as f:
        for line in f:
            match = re.match(r"^__version__ = ['\"]([^'\"]*)['\"]", line)
            if match:
                return match.group(1)
    raise RuntimeError("Version not found in dfb/__init__.py")


dfb_version = get_version()

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
    # Hack to replace the version. Make a copy, replace the text, then move the copy
    # back again.
    shutil.move("dfb/__init__.py", "_tmp.py")
    with open("_tmp.py") as fp:
        init = fp.read()
    init = init.replace("__git_version__ = None", f"__git_version__ = {git!r}")
    with open("dfb/__init__.py", "wt") as fp:
        fp.write(init)

    setup(
        name="dfb",
        packages=["dfb", "dfbmount", "dfblink"],
        long_description=long_description,
        install_requires=["requests"],
        entry_points={
            "console_scripts": [
                "dfb=dfb.cli:cli",
                "dfbshebanged=dfb.cli:clishebang",
                "dfb-mount=dfbmount.mount:cli",
                "dfb-link=dfblink.cli:cli",
            ],
        },
        version=dfb.__version__,  # Do not include git as per PEP440
        description="Dated File Backup",
        long_description_content_type="text/markdown",
        url="https://github.com/Jwink3101/dfb/",
        author="Justin Winokur",
        author_email="Jwink3101@users.noreply.github.com",
        license="MIT",
        python_requires=">=3.9",
    )
finally:
    shutil.move("_tmp.py", "dfb/__init__.py")
