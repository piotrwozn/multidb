#!/usr/bin/env python3
"""Refuse release tags that disagree with the version embedded in the binary."""

import argparse
from pathlib import Path
import re
import tomllib


def validate(tag: str, manifest: Path) -> str:
    version = tomllib.loads(manifest.read_text(encoding="utf-8"))["package"]["version"]
    if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?", tag):
        raise ValueError("release tag must be v followed by a semantic version")
    if tag != f"v{version}":
        raise ValueError(f"release tag {tag!r} does not match Cargo package version {version!r}")
    return version


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag")
    parser.add_argument("--manifest", type=Path, default=Path(__file__).resolve().parents[1] / "Cargo.toml")
    args = parser.parse_args()
    try:
        version = validate(args.tag, args.manifest)
    except (ValueError, KeyError, OSError) as error:
        parser.exit(1, f"release version check failed: {error}\n")
    print(f"release version verified: {version}")


if __name__ == "__main__":
    main()
