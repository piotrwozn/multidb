#!/usr/bin/env python3
"""Copy only hash-pinned, reviewed harness files into a fresh scratch directory."""
import argparse
import datetime
import json
from pathlib import Path
import shutil

from compare import HARNESSES, sha256


def prepare(source, destination):
    source, destination = Path(source).resolve(), Path(destination).resolve()
    # Verify everything before creating the destination or executing anything.
    for name, expected in HARNESSES.items():
        if sha256(source / name) != expected:
            raise ValueError(f"unreviewed harness contents: {name}")
    destination.mkdir(parents=True, exist_ok=False)
    files = [*HARNESSES]
    files.extend(name for name in ("requirements.txt", "README-methodology.md") if (source / name).is_file())
    hashes = {}
    for name in files:
        shutil.copyfile(source / name, destination / name)
        hashes[name] = sha256(destination / name)
        if name in HARNESSES and hashes[name] != HARNESSES[name]:
            raise ValueError(f"harness changed while copying: {name}")
    manifest = {
        "source_directory": str(source),
        "copied_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "reviewed_executables": list(HARNESSES), "files": hashes,
    }
    (destination / "provenance.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(prepare(args.source, args.destination), indent=2))
