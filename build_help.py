#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

COLUMNS = 88

env = os.environ.copy()
env["COLUMNS"] = str(COLUMNS)

os.chdir(os.path.dirname(__file__))

commands = """\
init
backup
refresh
restore-dir
restore-file
ls
snapshot
tree
versions
timestamps
prune
summary
advanced
advanced dbimport
advanced prune-file
advanced timestamp-include-filters
utils
utils apath2rpath
utils rpath2apath
"""

commands = [l.strip() for l in commands.split("\n") if l.strip()]
commands.insert(0, None)

helpmd = [
    "# CLI Help",
]

ver = (
    subprocess.check_output([sys.executable, "dfb.py", "version"], env=env)
    .strip()
    .decode()
)
pyver = subprocess.check_output([sys.executable, "--version"], env=env).strip().decode()
helpmd.append(
    f"""
version: `{ver}`  
"""
)

for command in commands:
    command = command or ""  # sill Falsy but can be used later to extend
    name = command or "No Command"
    helpmd.append(f"# {name}")

    cmd = [sys.executable, "dfb.py", *shlex.split(command), "--help"]

    help = subprocess.check_output(cmd, env=env)
    help = help.decode().replace("usage: dfb.py", "usage: dfb")

    helpmd.append(
        f"""
```text
{help}
```"""
    )

with open("docs/CLI_help.md", "wt") as f:
    f.write("\n\n".join(helpmd))

# Build the readme for docs
docs = Path("docs")

md = []
md.append("# Additional Documentation\n")
md.append("<!--- Auto Generated -->\n")

md2 = ["<!--- Auto Generated -->", "<!--- DO NOT MODIFY. WILL NOT BE SAVED -->"]

for file in sorted(docs.glob("*.md"), key=lambda p: p.name.lower()):
    if file.name == "readme.md":
        continue

    for line in file.read_text().splitlines():
        if not line.strip():
            continue
        title = line.strip().lstrip("#").strip()
        break
    else:
        continue  # empty file

    md.append(f"- [{title}]({file.name})")
    md2.append(f"- [{title}](docs/{file.name})")

(docs / "readme.md").write_text("\n".join(md))

with open("readme.md", "r") as rmin, open(".readme.md.swp", "wt") as rmout:
    for line in rmin:
        rmout.write(line)

        if not line.startswith("<!--- BEGIN AUTO GENERATED -->"):
            continue

        rmout.write("\n".join(md2))  # write md2

        for line in rmin:  # keep reading until we get our line
            if not line.startswith("<!--- END AUTO GENERATED -->"):
                continue
            rmout.write("\n<!--- END AUTO GENERATED -->\n")
            break
        else:
            raise ValueError("Did not find end sentinel")
shutil.move(".readme.md.swp", "readme.md")
