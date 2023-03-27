#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import subprocess

os.chdir(os.path.dirname(__file__))

commands = """\
init
backup
restore-dir
restore-file
ls
snapshot
versions
prune"""

commands = [l.strip() for l in commands.split("\n") if l.strip()]
commands.insert(0, None)

helpmd = [
    "# CLI Help",
]

for command in commands:
    name = command if command else "No Command"
    helpmd.append(f"# {name}")

    cmd = [sys.executable, "dfb.py"]
    if command:
        cmd.append(command)
    cmd.append("--help")

    help = (
        subprocess.check_output(cmd).decode().replace("usage: dfb.py", "usage: dfb")
    )  # long comment

    helpmd.append(
        f"""
```text
{help}
```"""
    )

with open("CLI_help.md", "wt") as f:
    f.write("\n\n".join(helpmd))
