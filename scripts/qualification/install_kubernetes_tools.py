#!/usr/bin/env python3
"""Install repository-pinned Linux/amd64 qualification tools over verified HTTPS.

The pins were checked against official release checksums/asset metadata on
2026-10-28. There are no caller-controlled URLs, hashes or archive paths. This
provenance concerns tool installation, not a Kubernetes deployment qualification.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import ssl
import subprocess
import tarfile
import tempfile
import time
import urllib.parse
import urllib.request


PINS = {
    "helm": {
        "version": "v4.3.1",
        "url": "https://get.helm.sh/helm-v4.3.1-linux-amd64.tar.gz",
        "download_sha256": "0403a88ae20913d11b10a2467b50087721fea7581736da6b9d3992f5357fe54b",
        "binary_sha256": "b66cb19f2b31f9d427632ea6c4b4a1ff89b74ecaeaee323cd166da41a7af53bd",
        "member": "linux-amd64/helm",
        "allowed_members": ("linux-amd64", "linux-amd64/helm", "linux-amd64/LICENSE", "linux-amd64/README.md"),
        "version_args": ("version", "--short"),
        "hosts": ("get.helm.sh",),
    },
    "kubeconform": {
        "version": "v0.8.0",
        "url": "https://github.com/yannh/kubeconform/releases/download/v0.8.0/kubeconform-linux-amd64.tar.gz",
        "download_sha256": "9bc2bffbf71f261128533edaf912153948b7ff238f9a531ae6d34466ec287883",
        "binary_sha256": "457722b36e98d7bdbc91525e594951ef5d7ea3a29da2d13b9b28a2fb8a5097b7",
        "member": "kubeconform",
        "allowed_members": ("kubeconform", "LICENSE", "LICENSE.md", "LICENSE.txt", "README", "README.md", "CHANGELOG.md"),
        "version_args": ("-v",),
        "hosts": ("github.com", "release-assets.githubusercontent.com", "objects.githubusercontent.com"),
    },
    "kind": {
        "version": "v0.33.0",
        "url": "https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-linux-amd64",
        "download_sha256": "aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d",
        "binary_sha256": "aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d",
        "member": None,
        "version_args": ("version",),
        "hosts": ("github.com", "release-assets.githubusercontent.com", "objects.githubusercontent.com"),
    },
    "kubectl": {
        "version": "v1.35.8",
        "url": "https://dl.k8s.io/release/v1.35.8/bin/linux/amd64/kubectl",
        "download_sha256": "874d5e72dbb819f43cff16bcd1e4f8bac5b7f2361fe1e55049b0a6c676fb0cbf",
        "binary_sha256": "874d5e72dbb819f43cff16bcd1e4f8bac5b7f2361fe1e55049b0a6c676fb0cbf",
        "member": None,
        "version_args": ("version", "--client", "-o", "json"),
        "hosts": ("dl.k8s.io", "cdn.dl.k8s.io"),
    },
}
MAX_DOWNLOAD = 64 * 1024 * 1024
MAX_BINARY = 128 * 1024 * 1024


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def check_url(url, pin):
    parsed = urllib.parse.urlsplit(url)
    require(parsed.scheme == "https" and parsed.hostname in pin["hosts"]
            and parsed.port in (None, 443) and parsed.username is None and parsed.password is None
            and not parsed.fragment, "tool download redirected outside its HTTPS host allowlist")


class VerifiedRedirect(urllib.request.HTTPRedirectHandler):
    def __init__(self, pin):
        super().__init__()
        self.pin = pin

    def redirect_request(self, request, response, code, message, headers, new_url):
        check_url(new_url, self.pin)
        return super().redirect_request(request, response, code, message, headers, new_url)


def download(pin, destination):
    check_url(pin["url"], pin)
    context = ssl.create_default_context()
    require(context.check_hostname and context.verify_mode == ssl.CERT_REQUIRED, "verified TLS is required")
    opener = urllib.request.build_opener(VerifiedRedirect(pin), urllib.request.HTTPSHandler(context=context))
    request = urllib.request.Request(pin["url"], headers={"User-Agent": "multidb-pinned-qualification-tools/1"})
    deadline, total = time.monotonic() + 120, 0
    with opener.open(request, timeout=15) as response, Path(destination).open("xb") as output:
        check_url(response.geturl(), pin)
        require(response.status == 200, "tool server did not return HTTP 200")
        length = response.headers.get("Content-Length")
        require(length is None or 0 < int(length) <= MAX_DOWNLOAD, "tool download Content-Length exceeds limit")
        while True:
            require(time.monotonic() < deadline, "tool download exceeded total deadline")
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            require(total <= MAX_DOWNLOAD, "tool download exceeds size limit")
            output.write(chunk)
    require(total > 0 and digest(destination) == pin["download_sha256"], "pinned tool archive checksum mismatch")


def extract_binary(archive_path, pin, destination):
    # Never process a tar header, let alone execute a file, until the complete
    # archive matches the repository-pinned official release checksum.
    require(digest(archive_path) == pin["download_sha256"], "pinned tool archive checksum mismatch")
    if pin["member"] is None:
        require(Path(archive_path).stat().st_size <= MAX_BINARY, "tool binary exceeds size limit")
        with Path(archive_path).open("rb") as source, Path(destination).open("xb") as target:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                target.write(chunk)
    else:
        seen, binary = set(), None
        with tarfile.open(archive_path, mode="r:gz", errorlevel=2) as archive:
            for member in archive:
                name = member.name.rstrip("/") if member.isdir() else member.name
                require(name in pin["allowed_members"] and name not in seen,
                        "tool archive has an unexpected or duplicate member")
                seen.add(name)
                require(member.isfile() or member.isdir(), "tool archive links and special members are forbidden")
                require(member.sparse is None and 0 <= member.size <= MAX_BINARY, "unsupported tool archive member size/type")
                require(not member.isdir() or member.size == 0, "tool archive directory has a payload")
                if name == pin["member"]:
                    require(member.isfile(), "tool binary is not a regular file")
                    binary = member
            require(binary is not None, "pinned executable member is missing")
            source = archive.extractfile(binary)
            require(source is not None, "pinned executable member cannot be read")
            with source, Path(destination).open("xb") as target:
                remaining = binary.size
                while remaining:
                    chunk = source.read(min(1024 * 1024, remaining))
                    require(chunk, "truncated pinned tool binary")
                    target.write(chunk)
                    remaining -= len(chunk)
    require(digest(destination) == pin["binary_sha256"], "pinned executable checksum mismatch")
    Path(destination).chmod(0o755)


def verify_version(name, path, pin):
    require(digest(path) == pin["binary_sha256"], "tool checksum changed before version check")
    result = subprocess.run([str(path), *pin["version_args"]], capture_output=True, text=True,
                            timeout=15, check=False)
    require(result.returncode == 0, f"{name} version command failed")
    output = result.stdout.strip()
    if name == "kubectl":
        require(json.loads(output).get("clientVersion", {}).get("gitVersion") == pin["version"], "unexpected kubectl version")
    elif name == "kind":
        require(re.fullmatch(r"kind " + re.escape(pin["version"]) + r"(?: [^\r\n]+)?", output), "unexpected Kind version")
    elif name == "helm":
        require(re.fullmatch(re.escape(pin["version"]) + r"(?:\+g[0-9a-f]+)?", output), "unexpected Helm version")
    else:
        require(output == pin["version"], "unexpected kubeconform version")
    require(digest(path) == pin["binary_sha256"], "tool checksum changed during version check")
    return output


def install(output, runtime=False):
    require(platform.system() == "Linux" and platform.machine() in ("x86_64", "amd64"),
            "pinned tools support Linux/amd64 only")
    output = Path(output).resolve()
    output.mkdir(parents=True, mode=0o700, exist_ok=False)
    names = ("helm", "kubeconform", "kind", "kubectl") if runtime else ("helm", "kubeconform")
    report = {"schema_version": 1, "kind": "pinned_kubernetes_tool_installation", "status": "failed",
              "platform": "linux/amd64", "pins_source": "official HTTPS release checksums/asset metadata verified 2026-10-28",
              "installer_sha256": digest(__file__), "runtime_tools": runtime, "tools": []}
    try:
        with tempfile.TemporaryDirectory(prefix=".staging-", dir=output) as temporary:
            staging = Path(temporary)
            for name in names:
                pin = PINS[name]
                archive, binary = staging / (name + ".download"), staging / name
                download(pin, archive)
                extract_binary(archive, pin, binary)
                version = verify_version(name, binary, pin)
                report["tools"].append({"tool": name, "url": pin["url"], "download_sha256": pin["download_sha256"],
                    "binary_sha256": pin["binary_sha256"], "version": version, "path": str(output / name)})
            for name in names:
                os.link(staging / name, output / name)
            report["status"] = "accepted"
    except Exception as error:
        report["reason"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        (output / "provenance.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def verify_installed(output, runtime=False):
    output = Path(output)
    require(output.is_dir() and not output.is_symlink(), "verified tool directory is required")
    names = ("helm", "kubeconform", "kind", "kubectl") if runtime else ("helm", "kubeconform")
    provenance = json.loads((output / "provenance.json").read_text())
    require(provenance.get("status") == "accepted"
            and provenance.get("installer_sha256") == digest(__file__), "tool artifact provenance does not match this installer")
    entries = {entry["tool"]: entry for entry in provenance["tools"]}
    require(len(entries) == len(provenance["tools"]) and set(names) <= set(entries), "tool artifact provenance is incomplete or duplicated")
    require(type(provenance.get("runtime_tools")) is bool, "tool artifact mode is missing")
    expected = set(PINS) if provenance["runtime_tools"] else {"helm", "kubeconform"}
    require(set(entries) == expected and {path.name for path in output.iterdir()} == expected | {"provenance.json"},
            "tool artifact contains unexpected or missing files")
    require(all(path.is_file() and not path.is_symlink() for path in output.iterdir()),
            "tool artifact links and directories are forbidden")
    tools = []
    for name in names:
        pin, binary = PINS[name], output / name
        require(binary.is_file() and not binary.is_symlink(), "tool artifact must contain regular executables")
        require(entries[name].get("url") == pin["url"]
                and entries[name].get("download_sha256") == pin["download_sha256"]
                and entries[name].get("binary_sha256") == pin["binary_sha256"], "tool artifact differs from repository pins")
        version = verify_version(name, binary, pin)
        tools.append({"tool": name, "path": str(binary.resolve()), "sha256": digest(binary), "version": version})
    return {"schema_version": 1, "status": "accepted", "kind": "pinned_kubernetes_tool_reverification", "tools": tools}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--output", type=Path)
    action.add_argument("--verify", type=Path, help="reverify a downloaded CI tool artifact without network access")
    parser.add_argument("--runtime", action="store_true", help="also install the pinned Kind and skew-compatible kubectl")
    args = parser.parse_args()
    report = verify_installed(args.verify, args.runtime) if args.verify else install(args.output, args.runtime)
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
