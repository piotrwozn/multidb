#!/usr/bin/env python3
"""Copy an accepted stable OCI image without rebuilding or changing its digest.

No credentials are accepted in arguments; ORAS uses the runner's registry config.
The CLI only targets the current GitHub repository's exact release-version tag.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import time
import tomllib

from install_release_tools import verify as verify_oras

SPEC = importlib.util.spec_from_file_location("image_release_readiness", Path(__file__).resolve().parents[1] / "check-production-readiness.py")
readiness = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(readiness)


def run(command, limit=8 * 1024 * 1024, timeout=180):
    """Bound the pinned tool's stdout/stderr and terminate its complete process group."""
    process = subprocess.Popen(list(map(str, command)), stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    buffers = {"stdout": bytearray(), "stderr": bytearray()}
    deadline = time.monotonic() + timeout
    succeeded = False
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ, "stdout")
            selector.register(process.stderr, selectors.EVENT_READ, "stderr")
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("ORAS command exceeded its deadline")
                for key, _events in selector.select(min(remaining, 1)):
                    chunk = os.read(key.fd, 64 * 1024)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    buffer = buffers[key.data]
                    if len(buffer) + len(chunk) > (limit if key.data == "stdout" else 1024 * 1024):
                        raise ValueError("ORAS output exceeded its bounded limit")
                    buffer.extend(chunk)
            code = process.wait(timeout=max(.001, deadline - time.monotonic()))
        if code:
            # Do not echo an external registry response or credential-bearing error.
            raise ValueError(f"ORAS command failed with exit code {code}")
        succeeded = True
        return bytes(buffers["stdout"])
    finally:
        # A child can keep inherited pipes open after its direct parent exits.
        # Failure must stop that owned group too, not only a still-live parent.
        if not succeeded:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=10)
        process.stdout.close()
        process.stderr.close()


def verify_remote(oras, reference, expected, runner=run):
    raw = runner([oras, "manifest", "fetch", reference])
    actual = "sha256:" + hashlib.sha256(raw).hexdigest()
    if actual != expected:
        raise ValueError("remote raw image manifest differs from the qualified OCI digest")
    return actual


def copy_verified(oras, archive, expected, destination, runner=run):
    """Push by digest, verify bytes, then promote the release tag and verify again."""
    repository, tag = destination.rsplit(":", 1)
    by_digest = repository + "@" + expected
    runner([oras, "cp", "--from-oci-layout", str(archive) + "@" + expected, by_digest])
    verify_remote(oras, by_digest, expected, runner)
    runner([oras, "tag", by_digest, tag])
    return verify_remote(oras, destination, expected, runner)


def image_reference(repository, version, git_tag):
    if not isinstance(repository, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*/[A-Za-z0-9][A-Za-z0-9_.-]*", repository):
        raise ValueError("validated GitHub repository context required")
    readiness.version_policy(version)
    if git_tag != "v" + version:
        raise ValueError("exact GitHub release tag context required")
    tag = "v" + version.replace("+", "_")
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag):
        raise ValueError("version cannot be represented by a bounded OCI image tag")
    return "ghcr.io/" + repository.lower() + ":" + tag


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--reference-only", action="store_true", help="print the validated registry reference without publishing")
    parser.add_argument("--evidence-root", type=Path)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--bundle", type=Path)
    parser.add_argument("--oras", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--verify-only", action="store_true", help="recheck the tag after attaching signatures/referrers")
    args = parser.parse_args(argv)
    try:
        repo = args.repo.resolve()
        version = tomllib.loads((repo / "Cargo.toml").read_text())["package"]["version"]
        reference = image_reference(os.environ.get("GITHUB_REPOSITORY"), version, os.environ.get("GITHUB_REF_NAME"))
        if args.reference_only:
            print(reference)
            return 0
        if not readiness.version_policy(version)["qualification_required"]:
            raise ValueError("qualified image publication only accepts stable versions")
        if not all((args.evidence_root, args.manifest, args.binary, args.bundle, args.oras, args.output)):
            raise ValueError("actual evidence, manifest, binary, bundle, pinned ORAS and fresh output are required")
        if args.output.exists() or args.output.is_symlink():
            raise ValueError("publication receipt requires a fresh output file")
        root = args.evidence_root.resolve()
        result = readiness.verify(repo, root, args.manifest, "linux-x86_64", args.binary.resolve(), version)
        bundle = readiness.verify_image_bundle(root, result, args.bundle)
        tool = verify_oras(args.oras)
        expected = bundle["manifest_digest"]
        if args.verify_only:
            verified = verify_remote(args.oras, reference, expected)
        else:
            verified = copy_verified(args.oras, bundle["archive"], expected, reference)
        output = {"schema_version": 1, "status": "accepted", "publication_mode": "qualified_oci_digest_copy",
                  "containerimage.digest": verified, "reference": reference, "archive_sha256": bundle["archive_sha256"],
                  "config_digest": bundle["config_digest"], "source": result["source"], "oras": tool,
                  "artifact_sha256": result["artifact_sha256"], "manifest_sha256": result["manifest_sha256"],
                  "scope": "remote image bytes verified; production acceptance remains the separately reviewed gates"}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x") as stream:
            stream.write(json.dumps(output, indent=2, sort_keys=True) + "\n")
        return 0
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError, TimeoutError) as error:
        print(json.dumps({"status": "blocked", "issues": [str(error)]}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
