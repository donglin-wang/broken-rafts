#!/usr/bin/env -S uv run --python 3.14t
import argparse
import importlib
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--version",
        default="v0",
        help="canonical implementation version to run, e.g. v0",
    )
    args = parser.parse_args()

    version_dir = Path(__file__).resolve().parent / "src" / args.version
    if not version_dir.is_dir():
        parser.error(f"unknown canonical implementation version: {args.version}")

    sys.path.insert(0, str(version_dir))
    raft = importlib.import_module("raft")
    raft.RaftNode().main()


if __name__ == "__main__":
    main()
