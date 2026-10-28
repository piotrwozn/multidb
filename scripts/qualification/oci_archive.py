#!/usr/bin/env python3
"""Convert one tagged Docker-save image into a deterministic OCI image archive.

No extraction or Docker invocation takes place. Outer-archive paths and member
types are untrusted. Layer filesystem links are preserved as image semantics;
the converter validates their tar streams without following or extracting them.
The OCI manifest digest is deliberately distinct from Docker's config/image ID.
"""

import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import tarfile
import tempfile


OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_INDEX = "application/vnd.oci.image.index.v1+json"
OCI_CONFIG = "application/vnd.oci.image.config.v1+json"
OCI_LAYER = "application/vnd.oci.image.layer.v1.tar"
CHUNK = 1024 * 1024
MAX_METADATA = 16 * 1024 * 1024
MAX_BYTES = 32 * 1024 * 1024 * 1024
MAX_MEMBERS = 100_000
MAX_TAR_METADATA = 64 * 1024 * 1024
HEX = re.compile(r"[a-f0-9]{64}\Z")


class OciArchiveError(ValueError):
    """The input cannot be authenticated as a supported image archive."""


class _BoundedTarInfo(tarfile.TarInfo):
    def _proc_member(self, archive):
        # tarfile processes GNU/PAX extension bodies before yielding TarInfo.
        # Bound those advertised lengths before its metadata reads/decoding.
        _require(self.type != tarfile.GNUTYPE_SPARSE, "GNU sparse tar members are unsupported")
        extensions = (tarfile.GNUTYPE_LONGNAME, tarfile.GNUTYPE_LONGLINK,
                      tarfile.XHDTYPE, tarfile.XGLTYPE)
        if self.type in extensions:
            _require(0 <= self.size <= 1024 * 1024, "tar extension exceeds metadata limit")
            total = getattr(archive, "_qualification_extension_bytes", 0) + self.size
            chain = getattr(archive, "_qualification_extension_chain", 0) + 1
            _require(total <= MAX_TAR_METADATA and chain <= 16, "too much chained tar metadata")
            archive._qualification_extension_bytes = total
            archive._qualification_extension_chain = chain
        else:
            archive._qualification_extension_chain = 0
        return super()._proc_member(archive)


def _require(condition, message):
    if not condition:
        raise OciArchiveError(message)


def _digest(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _json(data, context):
    def pairs(items):
        result = {}
        for key, value in items:
            _require(key not in result, f"duplicate JSON key in {context}: {key}")
            result[key] = value
        return result

    def constant(value):
        raise OciArchiveError(f"nonfinite JSON value in {context}: {value}")

    try:
        return json.loads(data.decode("utf-8"), object_pairs_hook=pairs,
                          parse_constant=constant)
    except (UnicodeError, json.JSONDecodeError, RecursionError) as error:
        raise OciArchiveError(f"invalid {context}: {error}") from error


def _path(name, directory=False, layer=False):
    _require(isinstance(name, str) and 0 < len(name) <= 4096,
             "invalid archive member name")
    _require(not any(ord(char) < 32 or ord(char) == 127 for char in name)
             and "\\" not in name and ":" not in name and not name.startswith("/"),
             f"unsafe archive member path: {name!r}")
    try:
        name.encode("utf-8", "strict")
    except UnicodeError as error:
        raise OciArchiveError("archive path is not UTF-8") from error
    if layer:
        while name.startswith("./"):
            name = name[2:]
        if directory and name in ("", "."):
            return "."
    if directory and name.endswith("/"):
        name = name[:-1]
    _require(all(part not in ("", ".", "..") for part in name.split("/")),
             f"noncanonical or traversing archive path: {name!r}")
    return name


def _inventory(archive):
    members, required_directories, total, metadata, last_end = {}, set(), 0, 0, 0
    for index, member in enumerate(archive):
        _require(index < MAX_MEMBERS, "too many Docker-save archive members")
        _require(member.isfile() or member.isdir(), "Docker-save outer archive contains a link or special member")
        _require(member.sparse is None, "sparse outer archive members are unsupported")
        name = _path(member.name, directory=member.isdir())
        _require(name not in members, f"duplicate outer archive member: {name}")
        _require(member.size >= 0, "negative archive member size")
        _require(not member.isdir() or member.size == 0, "directory member has a payload")
        metadata += 512 + len(name) + sum(128 + len(key) + len(value)
                                         for key, value in member.pax_headers.items())
        _require(metadata <= MAX_TAR_METADATA, "outer tar metadata exceeds limit")
        total += member.size
        _require(total <= MAX_BYTES, "Docker-save archive exceeds size limit")
        parts = name.split("/")
        for length in range(1, len(parts)):
            parent = "/".join(parts[:length])
            _require(parent not in members or members[parent].isdir(),
                     f"file/directory collision in archive: {parent}")
            required_directories.add(parent)
        _require(not member.isfile() or name not in required_directories,
                 f"file/directory collision in archive: {name}")
        members[name] = member
        last_end = max(last_end, member.offset_data + (member.size + 511) // 512 * 512)
    _require(last_end <= MAX_BYTES, "Docker-save archive exceeds size limit")
    archive.fileobj.seek(last_end)
    trailing = 0
    while chunk := archive.fileobj.read(CHUNK):
        trailing += len(chunk)
        _require(last_end + trailing <= MAX_BYTES, "archive padding exceeds size limit")
        _require(not any(chunk), "hidden nonzero bytes after Docker-save tar")
    _require(trailing >= 1024, "Docker-save tar is missing its end markers")
    return members


def _member(members, name):
    name = _path(name)
    member = members.get(name)
    _require(member is not None and member.isfile(), f"missing regular archive member: {name}")
    return member


def _metadata(archive, member):
    _require(member.size <= MAX_METADATA, f"metadata too large: {member.name}")
    stream = archive.extractfile(member)
    _require(stream is not None, f"unreadable archive member: {member.name}")
    with stream:
        data = stream.read(MAX_METADATA + 1)
    _require(len(data) == member.size, f"truncated archive member: {member.name}")
    return data


def _copy(source, destination, maximum=MAX_BYTES):
    digest, size = hashlib.sha256(), 0
    while chunk := source.read(CHUNK):
        size += len(chunk)
        _require(size <= maximum, "layer exceeds uncompressed size limit")
        digest.update(chunk)
        destination.write(chunk)
    return "sha256:" + digest.hexdigest(), size


def _validate_layer(path):
    size, last_end, names = path.stat().st_size, 0, set()
    _require(size >= 1024 and size % 512 == 0, "layer is not a complete tar stream")
    metadata = 0
    with tarfile.open(path, mode="r:", errorlevel=2, tarinfo=_BoundedTarInfo) as archive:
        for index, member in enumerate(archive):
            _require(index < MAX_MEMBERS, "too many entries in a layer")
            name = _path(member.name, directory=member.isdir(), layer=True)
            _require(name not in names, f"duplicate layer member: {name}")
            names.add(name)
            metadata += 512 + len(name) + sum(128 + len(key) + len(value)
                                             for key, value in member.pax_headers.items())
            _require(metadata <= MAX_TAR_METADATA, "layer metadata exceeds limit")
            _require(member.sparse is None, "sparse layer format is unsupported")
            _require(member.size >= 0, "negative layer member size")
            end = member.offset_data + (member.size + 511) // 512 * 512
            _require(end <= size, "truncated layer payload")
            last_end = max(last_end, end)
    # tarfile accepts the first end marker and ignores trailing bytes. A layer
    # must contain its complete terminator, and no hidden second archive/junk.
    _require(size - last_end >= 1024, "layer is missing its end markers")
    with path.open("rb") as stream:
        stream.seek(last_end)
        while chunk := stream.read(CHUNK):
            _require(not any(chunk), "nonzero trailing bytes after layer tar")


def _expected_config_digest(name):
    parts = name.split("/")
    if len(parts) == 1 and name.endswith(".json") and HEX.fullmatch(name[:-5]):
        return "sha256:" + name[:-5]
    if len(parts) == 3 and parts[:2] == ["blobs", "sha256"] and HEX.fullmatch(parts[2]):
        return "sha256:" + parts[2]
    raise OciArchiveError("unsupported Docker config path (expected a SHA256 filename)")


def _layer(archive, member, directory, sequence):
    path = directory / f"layer-{sequence}.blob"
    source = archive.extractfile(member)
    _require(source is not None, "unreadable layer")
    with source, path.open("wb") as output:
        blob_digest, size = _copy(source, output)
    _require(size == member.size, "truncated Docker layer member")
    if member.name.startswith("blobs/sha256/"):
        _require(member.name == "blobs/" + blob_digest.replace(":", "/"),
                 "layer blob filename digest mismatch")
    with path.open("rb") as stream:
        magic = stream.read(4)
    media_type, uncompressed = OCI_LAYER, path
    if magic.startswith(b"\x1f\x8b"):
        media_type += "+gzip"
        uncompressed = directory / f"layer-{sequence}.tar"
        with gzip.open(path, "rb") as source, uncompressed.open("wb") as output:
            diff_id, _ = _copy(source, output)
    else:
        _require(magic != b"\x28\xb5\x2f\xfd", "zstd Docker-save layers are unsupported by this stdlib converter")
        diff_id = blob_digest
    _validate_layer(uncompressed)
    return {"mediaType": media_type, "digest": blob_digest, "size": size}, diff_id, path


def _add(archive, name, size, source=None, directory=False):
    member = tarfile.TarInfo(name)
    member.size = size
    member.mode = 0o755 if directory else 0o644
    member.type = tarfile.DIRTYPE if directory else tarfile.REGTYPE
    member.uid = member.gid = member.mtime = 0
    member.uname = member.gname = ""
    archive.addfile(member, source)


def convert(source_docker_tar, dest_oci_tar, reference):
    """Return exact OCI/config/layer digests and SHA256 of the published archive.

    Supports classic Docker save and Docker's blobs/sha256 path layout, with
    uncompressed or gzip tar layers. The sole source RepoTag must equal reference.
    Existing destinations are never overwritten. Limits bound metadata/container
    counts and decompression; layers are spooled on disk, not retained in RAM.
    """
    _require(isinstance(reference, str) and len(reference) <= 512 and "@" not in reference
             and not any(char.isspace() or ord(char) < 32 for char in reference), "invalid image reference")
    final_component = reference.rsplit("/", 1)[-1]
    _require(":" in final_component and re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", final_component.rsplit(":", 1)[1]),
             "reference must include an explicit Docker tag")
    source, destination = Path(source_docker_tar), Path(dest_oci_tar)
    _require(source.resolve() != destination.resolve(), "source and destination must differ")
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with tempfile.TemporaryDirectory(prefix=".oci-convert-", dir=destination.parent) as temporary:
            directory = Path(temporary)
            with tarfile.open(source, mode="r:*", errorlevel=2, tarinfo=_BoundedTarInfo) as archive:
                members = _inventory(archive)
                images = _json(_metadata(archive, _member(members, "manifest.json")), "Docker manifest")
                _require(isinstance(images, list) and len(images) == 1 and isinstance(images[0], dict), "expected exactly one Docker-save image")
                image = images[0]
                _require(image.get("RepoTags") == [reference], "expected exactly one source tag equal to reference")
                config_name = _path(image.get("Config"))
                config_bytes = _metadata(archive, _member(members, config_name))
                config_digest = _digest(config_bytes)
                _require(config_digest == _expected_config_digest(config_name), "Docker config filename digest mismatch")
                config = _json(config_bytes, "image config")
                _require(isinstance(config, dict), "image config must be an object")
                rootfs = config.get("rootfs")
                _require(isinstance(rootfs, dict) and rootfs.get("type") == "layers", "unsupported image rootfs")
                layers, diff_ids = image.get("Layers"), rootfs.get("diff_ids")
                _require(isinstance(layers, list) and isinstance(diff_ids, list) and len(layers) == len(diff_ids)
                         and len(layers) <= MAX_MEMBERS, "layer/rootfs.diff_ids count mismatch")
                platform = {key: config.get(key) for key in ("architecture", "os")}
                _require(all(isinstance(value, str) and value and len(value) <= 128 for value in platform.values()), "missing image platform")
                if "variant" in config:
                    _require(isinstance(config["variant"], str), "invalid platform variant")
                    platform["variant"] = config["variant"]
                blobs, descriptors, cache = {config_digest: config_bytes}, [], {}
                for number, (name, expected) in enumerate(zip(layers, diff_ids)):
                    name = _path(name)
                    _require(isinstance(expected, str) and expected.startswith("sha256:") and HEX.fullmatch(expected[7:]), "invalid rootfs.diff_id")
                    if name not in cache:
                        cache[name] = _layer(archive, _member(members, name), directory, number)
                    descriptor, observed, path = cache[name]
                    _require(observed == expected, f"rootfs.diff_id mismatch for {name}")
                    descriptors.append(descriptor)
                    blobs[descriptor["digest"]] = path

            manifest = _canonical({"schemaVersion": 2, "mediaType": OCI_MANIFEST,
                "config": {"mediaType": OCI_CONFIG, "digest": config_digest, "size": len(config_bytes)}, "layers": descriptors})
            manifest_digest = _digest(manifest)
            blobs[manifest_digest] = manifest
            index = _canonical({"schemaVersion": 2, "mediaType": OCI_INDEX, "manifests": [{
                "mediaType": OCI_MANIFEST, "digest": manifest_digest, "size": len(manifest),
                "platform": platform, "annotations": {"org.opencontainers.image.ref.name": reference}}]})
            staging = directory / "image.oci.tar"
            with tarfile.open(staging, mode="w:", format=tarfile.USTAR_FORMAT) as output:
                for name, data in [("oci-layout", _canonical({"imageLayoutVersion": "1.0.0"})), ("index.json", index)]:
                    _add(output, name, len(data), io.BytesIO(data))
                _add(output, "blobs", 0, directory=True)
                _add(output, "blobs/sha256", 0, directory=True)
                for digest in sorted(blobs):
                    value, name = blobs[digest], "blobs/" + digest.replace(":", "/")
                    if isinstance(value, bytes):
                        _add(output, name, len(value), io.BytesIO(value))
                    else:
                        with value.open("rb") as stream:
                            _add(output, name, value.stat().st_size, stream)
            with staging.open("rb") as stream:
                archive_sha256 = hashlib.file_digest(stream, "sha256").hexdigest()
                os.fsync(stream.fileno())
            os.link(staging, destination)  # atomic no-clobber publication
            return {"manifest_digest": manifest_digest, "archive_sha256": archive_sha256,
                    "config_digest": config_digest, "layer_digests": [item["digest"] for item in descriptors],
                    "reference": reference}
    except (tarfile.TarError, EOFError, gzip.BadGzipFile) as error:
        raise OciArchiveError(f"invalid Docker-save archive or layer: {error}") from error


def verify_oci_archive(path, evidence):
    """Bind a single Linux/amd64 OCI archive to its converter evidence.

    Returns the five fields produced by ``convert`` after checking the archive
    bytes, index, manifest, config and every referenced blob. Layer blobs are
    streamed for their distribution digest, without extracting or decompressing
    their filesystem contents. This verifies OCI identity, not runtime behavior.
    Additional caller report fields are ignored. Nothing is written or executed.
    """
    fields = ("manifest_digest", "archive_sha256", "config_digest", "layer_digests", "reference")
    _require(isinstance(evidence, dict) and all(key in evidence for key in fields),
             "complete OCI converter evidence is required")
    for key in ("manifest_digest", "config_digest"):
        _require(isinstance(evidence[key], str) and evidence[key].startswith("sha256:")
                 and HEX.fullmatch(evidence[key][7:]), "invalid OCI evidence digest")
    _require(isinstance(evidence["archive_sha256"], str) and HEX.fullmatch(evidence["archive_sha256"]),
             "invalid OCI archive checksum")
    expected_layers = evidence["layer_digests"]
    _require(isinstance(expected_layers, list) and len(expected_layers) <= MAX_MEMBERS
             and all(isinstance(value, str) and value.startswith("sha256:") and HEX.fullmatch(value[7:])
                     for value in expected_layers), "invalid OCI layer evidence")
    reference = evidence["reference"]
    _require(isinstance(reference, str) and len(reference) <= 512 and "@" not in reference
             and not any(char.isspace() or ord(char) < 32 for char in reference), "invalid OCI reference")
    final_component = reference.rsplit("/", 1)[-1]
    _require(":" in final_component and re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", final_component.rsplit(":", 1)[1]),
             "OCI reference must include an explicit tag")
    path = Path(path)
    _require(path.is_file() and not path.is_symlink() and path.stat().st_size <= MAX_BYTES,
             "a bounded regular OCI archive is required")
    try:
        with path.open("rb") as source:
            initial = os.fstat(source.fileno())
            _require(initial.st_size <= MAX_BYTES, "OCI archive exceeds size limit")
            archive_hash, archive_size = hashlib.sha256(), 0
            while chunk := source.read(CHUNK):
                archive_size += len(chunk)
                _require(archive_size <= initial.st_size, "OCI archive grew during verification")
                archive_hash.update(chunk)
            _require(archive_size == initial.st_size, "OCI archive changed during verification")
            archive_sha256 = archive_hash.hexdigest()
            _require(archive_sha256 == evidence["archive_sha256"], "OCI archive checksum mismatch")
            source.seek(0)
            with tarfile.open(fileobj=source, mode="r:", errorlevel=2, tarinfo=_BoundedTarInfo) as archive:
                members = _inventory(archive)
                for name, member in members.items():
                    if member.isdir():
                        _require(name in ("blobs", "blobs/sha256"), "unexpected OCI archive directory")
                    else:
                        _require(name in ("oci-layout", "index.json")
                                 or (name.startswith("blobs/sha256/") and HEX.fullmatch(name[13:])),
                                 "unexpected OCI archive member")
                layout = _json(_metadata(archive, _member(members, "oci-layout")), "OCI layout")
                _require(layout == {"imageLayoutVersion": "1.0.0"}, "unsupported OCI layout")
                index = _json(_metadata(archive, _member(members, "index.json")), "OCI index")
                _require(isinstance(index, dict) and set(index) == {"schemaVersion", "mediaType", "manifests"}
                         and type(index["schemaVersion"]) is int and index["schemaVersion"] == 2
                         and index["mediaType"] == OCI_INDEX and isinstance(index["manifests"], list)
                         and len(index["manifests"]) == 1, "expected exactly one OCI image manifest")
                descriptor = index["manifests"][0]
                _require(isinstance(descriptor, dict)
                         and set(descriptor) == {"mediaType", "digest", "size", "platform", "annotations"}
                         and descriptor["platform"] == {"architecture": "amd64", "os": "linux"}
                         and descriptor["annotations"] == {"org.opencontainers.image.ref.name": reference},
                         "OCI image platform/reference mismatch")
                referenced, verified = set(), {}

                def blob(item, allowed_media, metadata=False):
                    _require(isinstance(item, dict) and item.get("mediaType") in allowed_media
                             and isinstance(item.get("digest"), str) and item["digest"].startswith("sha256:")
                             and HEX.fullmatch(item["digest"][7:]) and type(item.get("size")) is int
                             and 0 < item["size"] <= (MAX_METADATA if metadata else MAX_BYTES),
                             "invalid OCI blob descriptor")
                    name = "blobs/" + item["digest"].replace(":", "/")
                    member = _member(members, name)
                    _require(member.size == item["size"], "OCI descriptor size mismatch")
                    referenced.add(name)
                    data = _metadata(archive, member) if metadata else None
                    if name not in verified:
                        if data is not None:
                            actual, size = _digest(data), len(data)
                        else:
                            stream = archive.extractfile(member)
                            _require(stream is not None, "unreadable OCI blob")
                            digest, size = hashlib.sha256(), 0
                            with stream:
                                while chunk := stream.read(CHUNK):
                                    size += len(chunk)
                                    _require(size <= member.size, "OCI blob exceeds descriptor size")
                                    digest.update(chunk)
                            actual = "sha256:" + digest.hexdigest()
                        _require(size == member.size and actual == item["digest"], "OCI blob digest mismatch")
                        verified[name] = actual
                    return data

                manifest = _json(blob(descriptor, (OCI_MANIFEST,), metadata=True), "OCI manifest")
                _require(descriptor["digest"] == evidence["manifest_digest"], "OCI manifest evidence mismatch")
                _require(isinstance(manifest, dict) and set(manifest) == {"schemaVersion", "mediaType", "config", "layers"}
                         and type(manifest["schemaVersion"]) is int and manifest["schemaVersion"] == 2
                         and manifest["mediaType"] == OCI_MANIFEST
                         and isinstance(manifest["layers"], list) and len(manifest["layers"]) <= MAX_MEMBERS,
                         "unsupported OCI image manifest")
                config_descriptor = manifest["config"]
                _require(isinstance(config_descriptor, dict) and set(config_descriptor) == {"mediaType", "digest", "size"},
                         "invalid OCI config descriptor")
                config = _json(blob(config_descriptor, (OCI_CONFIG,), metadata=True), "OCI config")
                _require(config_descriptor["digest"] == evidence["config_digest"]
                         and config_descriptor["digest"] != descriptor["digest"], "OCI config evidence mismatch")
                _require(isinstance(config, dict) and config.get("architecture") == "amd64" and config.get("os") == "linux",
                         "OCI config platform mismatch")
                rootfs = config.get("rootfs")
                _require(isinstance(rootfs, dict) and rootfs.get("type") == "layers"
                         and isinstance(rootfs.get("diff_ids"), list)
                         and len(rootfs["diff_ids"]) == len(manifest["layers"])
                         and all(isinstance(value, str) and value.startswith("sha256:") and HEX.fullmatch(value[7:])
                                 for value in rootfs["diff_ids"]), "invalid OCI config rootfs")
                layer_digests = []
                for item, diff_id in zip(manifest["layers"], rootfs["diff_ids"]):
                    _require(isinstance(item, dict) and set(item) == {"mediaType", "digest", "size"},
                             "invalid OCI layer descriptor")
                    blob(item, (OCI_LAYER, OCI_LAYER + "+gzip"))
                    if item["mediaType"] == OCI_LAYER:
                        _require(diff_id == item["digest"], "uncompressed OCI layer diff_id mismatch")
                    layer_digests.append(item["digest"])
                _require(layer_digests == expected_layers, "OCI layer evidence mismatch")
                actual_blobs = {name for name, member in members.items() if member.isfile() and name.startswith("blobs/")}
                _require(referenced == actual_blobs, "unreferenced OCI blobs are forbidden")
            final = os.fstat(source.fileno())
            _require((initial.st_size, initial.st_mtime_ns, initial.st_ctime_ns)
                     == (final.st_size, final.st_mtime_ns, final.st_ctime_ns), "OCI archive changed during verification")
        return {"manifest_digest": descriptor["digest"], "archive_sha256": archive_sha256,
                "config_digest": config_descriptor["digest"], "layer_digests": layer_digests, "reference": reference}
    except (tarfile.TarError, EOFError) as error:
        raise OciArchiveError(f"invalid OCI archive: {error}") from error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--reference", required=True)
    args = parser.parse_args()
    print(json.dumps(convert(args.source, args.destination, args.reference), sort_keys=True))


if __name__ == "__main__":
    main()
