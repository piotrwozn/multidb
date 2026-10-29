#!/usr/bin/env python3
"""Install the pinned Linux/amd64 ORAS used for digest-preserving publication."""
import argparse
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import tempfile

from install_kubernetes_tools import digest, download, extract_binary, require

PIN = {
    "version": "1.3.6",
    "url": "https://github.com/oras-project/oras/releases/download/v1.3.6/oras_1.3.6_linux_amd64.tar.gz",
    "download_sha256": "66fb0ad7206f3f6fa77ed5b552049d364ccf4b50b1684554f731f76302b52ca0",
    "binary_sha256": "2be7e2b668fc32329408ca17f104021bdb93407266537b6410668ed7d05c4611",
    "member": "oras",
    "allowed_members": ("oras", "LICENSE"),
    "hosts": ("github.com", "release-assets.githubusercontent.com", "objects.githubusercontent.com"),
}


def verify(path):
    path = Path(path)
    require(path.is_file() and not path.is_symlink() and digest(path) == PIN["binary_sha256"],
            "ORAS executable differs from the repository pin")
    result = subprocess.run([str(path), "version"], capture_output=True, text=True, timeout=15, check=False)
    require(result.returncode == 0 and re.search(r"^Version:\s+" + re.escape(PIN["version"]) + r"$", result.stdout, re.M),
            "unexpected ORAS version")
    require(digest(path) == PIN["binary_sha256"], "ORAS changed during version verification")
    return {"tool": "oras", "version": PIN["version"], "sha256": PIN["binary_sha256"]}


def install(output):
    require(platform.system() == "Linux" and platform.machine() in ("x86_64", "amd64"),
            "pinned release tools support Linux/amd64 only")
    output = Path(output).resolve()
    output.mkdir(parents=True, mode=0o700, exist_ok=False)
    report = {"schema_version": 1, "kind": "pinned_release_tool_installation", "status": "failed",
              "installer_sha256": digest(__file__), "pins_source": "official ORAS release checksums and asset metadata"}
    try:
        with tempfile.TemporaryDirectory(prefix=".staging-", dir=output) as temporary:
            staging = Path(temporary)
            download(PIN, staging / "oras.tar.gz")
            extract_binary(staging / "oras.tar.gz", PIN, staging / "oras")
            report["tool"] = verify(staging / "oras")
            os.link(staging / "oras", output / "oras")
        report["status"] = "accepted"
    except Exception as error:
        report["reason"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        (output / "provenance.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--output", type=Path)
    action.add_argument("--verify", type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.verify) if args.verify else install(args.output), sort_keys=True))


if __name__ == "__main__":
    main()
