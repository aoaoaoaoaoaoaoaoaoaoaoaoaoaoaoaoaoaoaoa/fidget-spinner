#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Run the workspace's manifest-owned gates without touching the live ledger."""

import argparse
import subprocess
import tomllib
from collections.abc import Mapping
from pathlib import Path

ROOT = Path(__file__).resolve().parent
Command = tuple[str, ...]


def table(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise SystemExit(f"[check] invalid {name}: expected a table")
    return value


def command(value: object, name: str) -> Command:
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(part, str) and part for part in value)
    ):
        raise SystemExit(f"[check] invalid {name}: expected a non-empty string list")
    return tuple(value)


def run(name: str, argv: Command) -> None:
    print(f"[check] {name}: {' '.join(argv)}", flush=True)
    subprocess.run(argv, cwd=ROOT, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode",
        nargs="?",
        choices=("check", "verify", "deep", "fix", "canon", "install"),
        default="check",
        help="Check without edits; deep adds docs and isolated CLI proofs; fix and install are explicit",
    )
    args = parser.parse_args()
    with (ROOT / "Cargo.toml").open("rb") as source:
        manifest = tomllib.load(source)
    workspace = table(manifest.get("workspace"), "workspace")
    metadata = table(workspace.get("metadata"), "workspace.metadata")
    gates = table(metadata.get("rust-starter"), "workspace.metadata.rust-starter")

    if args.mode in {"fix", "canon"}:
        commands = gates.get("canonicalize_commands")
        if not isinstance(commands, list) or not commands:
            raise SystemExit("[check] canonicalize_commands must be a non-empty list")
        for index, value in enumerate(commands, start=1):
            run(f"canonicalize.{index}", command(value, "canonicalize_commands"))
        return
    if args.mode == "install":
        run("install", command(gates.get("install_command"), "install_command"))
        return

    names = ("format", "clippy", "test")
    if args.mode == "deep":
        names += ("doc", "runtime")
    for name in names:
        key = f"{name}_command"
        run(name, command(gates.get(key), key))


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        raise SystemExit(error.returncode) from error
    except KeyboardInterrupt:
        raise SystemExit(130) from None
