"""Tiny synthetic tar fixtures only. These tests build/run no container image."""

import gzip
import copy
import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest import mock

import oci_archive


REFERENCE = "docker.io/library/multidb-qualification:synthetic-test-only"


def sha(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def layer_tar(names=("app",), symlink=False):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:", format=tarfile.USTAR_FORMAT) as archive:
        for name in names:
            member = tarfile.TarInfo(name)
            data = b"synthetic fixture; not an executable image"
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
        if symlink:
            member = tarfile.TarInfo("app-link")
            member.type, member.linkname = tarfile.SYMTYPE, "app"
            archive.addfile(member)
    return buffer.getvalue()


def docker_entries(*, modern=False, compressed=False, diff_id=None,
                   config_override=None, tags=None, images=1, layer=None):
    raw = layer if layer is not None else layer_tar(symlink=True)
    blob = gzip.compress(raw, mtime=123) if compressed else raw
    config = {"architecture": "amd64", "os": "linux",
              "rootfs": {"type": "layers", "diff_ids": [diff_id or sha(raw)]}}
    config_data = json.dumps(config, sort_keys=True).encode()
    config_name = ("blobs/sha256/" + sha(config_data)[7:]) if modern else sha(config_data)[7:] + ".json"
    layer_name = ("blobs/sha256/" + sha(blob)[7:]) if modern else "legacy-chain-id/layer.tar"
    manifest = {"Config": config_name, "RepoTags": [REFERENCE] if tags is None else tags,
                "Layers": [layer_name]}
    entries = [(config_name, config_override if config_override is not None else config_data),
               (layer_name, blob), ("manifest.json", json.dumps([manifest] * images).encode())]
    return entries, config_data, raw, blob


def save(path, entries):
    with tarfile.open(path, mode="w:", format=tarfile.USTAR_FORMAT) as archive:
        for name, data in entries:
            if isinstance(name, tarfile.TarInfo):
                member = name
            else:
                member = tarfile.TarInfo(name)
                member.size = len(data)
            archive.addfile(member, io.BytesIO(data) if data is not None else None)


class OciArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.source = self.directory / "source.docker.tar"
        self.destination = self.directory / "image.oci.tar"

    def reject(self, entries, message=None):
        save(self.source, entries)
        with self.assertRaisesRegex(oci_archive.OciArchiveError, message or ".*"):
            oci_archive.convert(self.source, self.destination, REFERENCE)
        self.assertFalse(self.destination.exists())
        self.assertEqual(list(self.directory.glob(".oci-convert-*")), [])

    def test_real_oci_layout_hashes_actual_bytes_and_distinguishes_manifest_from_image_id(self):
        entries, config, raw, _ = docker_entries()
        save(self.source, entries)
        result = oci_archive.convert(self.source, self.destination, REFERENCE)
        self.assertNotEqual(result["manifest_digest"], result["config_digest"])
        self.assertEqual(result["config_digest"], sha(config))
        self.assertEqual(result["layer_digests"], [sha(raw)])
        self.assertEqual(result["archive_sha256"], hashlib.sha256(self.destination.read_bytes()).hexdigest())
        self.assertEqual(result["reference"], REFERENCE)
        with tarfile.open(self.destination) as archive:
            def read(name):
                with archive.extractfile(name) as stream:
                    return stream.read()
            self.assertEqual(json.loads(read("oci-layout")), {"imageLayoutVersion": "1.0.0"})
            index = json.loads(read("index.json"))
            descriptor = index["manifests"][0]
            manifest_bytes = read("blobs/" + descriptor["digest"].replace(":", "/"))
            self.assertEqual(sha(manifest_bytes), result["manifest_digest"])
            self.assertEqual(len(manifest_bytes), descriptor["size"])
            self.assertEqual(descriptor["platform"], {"architecture": "amd64", "os": "linux"})
            self.assertEqual(descriptor["annotations"]["org.opencontainers.image.ref.name"], REFERENCE)
            manifest = json.loads(manifest_bytes)
            self.assertEqual(manifest["mediaType"], oci_archive.OCI_MANIFEST)
            for item in [manifest["config"], *manifest["layers"]]:
                data = read("blobs/" + item["digest"].replace(":", "/"))
                self.assertEqual(sha(data), item["digest"])
                self.assertEqual(len(data), item["size"])
            self.assertEqual(read("blobs/" + sha(raw).replace(":", "/")), raw)
            for member in archive:
                self.assertEqual((member.uid, member.gid, member.mtime), (0, 0, 0))
        second = self.directory / "second.oci.tar"
        other = oci_archive.convert(self.source, second, REFERENCE)
        self.assertEqual(result, other)
        self.assertEqual(self.destination.read_bytes(), second.read_bytes())

    def test_current_blob_paths_and_gzip_keep_distribution_digest_separate_from_diff_id(self):
        entries, config, raw, blob = docker_entries(modern=True, compressed=True)
        save(self.source, entries)
        result = oci_archive.convert(self.source, self.destination, REFERENCE)
        self.assertEqual(result["config_digest"], sha(config))
        self.assertEqual(result["layer_digests"], [sha(blob)])
        self.assertNotEqual(sha(raw), sha(blob))
        with tarfile.open(self.destination) as archive:
            with archive.extractfile("blobs/" + result["manifest_digest"].replace(":", "/")) as stream:
                manifest = json.load(stream)
            self.assertEqual(manifest["layers"][0]["mediaType"], oci_archive.OCI_LAYER + "+gzip")

    def test_config_filename_and_diff_id_mismatches_fail_before_publication(self):
        entries, _, _, _ = docker_entries(config_override=b"{}")
        self.reject(entries, "config filename digest mismatch")
        entries, _, _, _ = docker_entries(diff_id="sha256:" + "0" * 64)
        self.reject(entries, "diff_id mismatch")

    def test_content_addressed_layer_filename_must_match_actual_blob(self):
        entries, _, _, _ = docker_entries(modern=True)
        old = entries[1][0]
        wrong = "blobs/sha256/" + "0" * 64
        manifest = json.loads(entries[2][1])
        manifest[0]["Layers"] = [wrong]
        entries[1] = (wrong, entries[1][1])
        entries[2] = ("manifest.json", json.dumps(manifest).encode())
        self.assertNotEqual(old, wrong)
        self.reject(entries, "layer blob filename digest mismatch")

    def test_traversal_and_noncanonical_outer_members_are_rejected(self):
        entries, _, _, _ = docker_entries()
        for name in ["../escape", "/escape", "a/../escape", "a//escape", "a\\escape", "C:escape", "./manifest.json"]:
            with self.subTest(name=name):
                self.reject([*entries, (name, b"x")], "path")

    def test_outer_links_and_special_members_are_rejected(self):
        entries, _, _, _ = docker_entries()
        for kind in [tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.CHRTYPE, tarfile.FIFOTYPE]:
            with self.subTest(kind=kind):
                member = tarfile.TarInfo("unsafe")
                member.type, member.linkname = kind, "../outside"
                self.reject([*entries, (member, None)], "link or special")

    def test_duplicate_outer_members_and_file_directory_collisions_are_rejected(self):
        entries, _, _, _ = docker_entries()
        self.reject([*entries, entries[-1]], "duplicate outer")
        for collision in [[("dir", b"x"), ("dir/file", b"y")],
                          [("dir/file", b"y"), ("dir", b"x")]]:
            self.reject([*entries, *collision], "collision")

    def test_single_source_image_and_exact_single_tag_are_required(self):
        for kwargs in [{"images": 2}, {"tags": []}, {"tags": [REFERENCE, "other:tag"]}, {"tags": ["other:tag"]}]:
            with self.subTest(kwargs=kwargs):
                entries, _, _, _ = docker_entries(**kwargs)
                self.reject(entries, "exactly one")

    def test_layer_header_traversal_duplicates_and_invalid_tar_are_rejected(self):
        for raw in [layer_tar(("../escape",)), layer_tar(("app", "app")), b"invalid tar"]:
            with self.subTest(length=len(raw)):
                entries, _, _, _ = docker_entries(layer=raw)
                self.reject(entries)

    def test_ubuntu_package_colon_filename_is_preserved_inside_the_layer(self):
        name = "var/lib/dpkg/info/gcc-14-base:amd64.list"
        raw = layer_tar((name,))
        entries, _, _, _ = docker_entries(layer=raw)
        save(self.source, entries)
        result = oci_archive.convert(self.source, self.destination, REFERENCE)
        self.assertEqual(result["layer_digests"], [sha(raw)])
        self.assertEqual(oci_archive.verify_oci_archive(self.destination, result), result)
        with tarfile.open(self.destination, mode="r:") as archive:
            with archive.extractfile("blobs/" + sha(raw).replace(":", "/")) as stream:
                with tarfile.open(fileobj=io.BytesIO(stream.read()), mode="r:") as layer:
                    self.assertEqual(layer.getnames(), [name])

    def test_colon_remains_forbidden_in_outer_archive_members(self):
        entries, _, _, _ = docker_entries()
        self.reject([*entries, ("var/lib/dpkg/info/gcc-14-base:amd64.list", b"outer")], "path")

    def test_hidden_trailing_outer_archive_content_is_rejected(self):
        entries, _, _, _ = docker_entries()
        save(self.source, entries)
        with self.source.open("ab") as stream:
            stream.write(b"hidden second archive" + b"\0" * 1024)
        with self.assertRaisesRegex(oci_archive.OciArchiveError, "hidden nonzero"):
            oci_archive.convert(self.source, self.destination, REFERENCE)
        self.assertFalse(self.destination.exists())

    def test_tar_extension_advertised_size_is_bounded_before_metadata_read(self):
        member = tarfile.TarInfo("PaxHeader")
        member.type, member.size = tarfile.XHDTYPE, 1 << 40
        self.source.write_bytes(member.tobuf(format=tarfile.GNU_FORMAT) + b"\0" * 1024)
        with self.assertRaisesRegex(oci_archive.OciArchiveError, "extension exceeds"):
            oci_archive.convert(self.source, self.destination, REFERENCE)
        self.assertFalse(self.destination.exists())

    def test_existing_or_concurrently_created_destinations_are_never_overwritten(self):
        entries, _, _, _ = docker_entries()
        save(self.source, entries)
        self.destination.write_bytes(b"existing")
        with self.assertRaises(FileExistsError):
            oci_archive.convert(self.source, self.destination, REFERENCE)
        self.assertEqual(self.destination.read_bytes(), b"existing")
        self.destination.unlink()
        def collision(_source, destination):
            Path(destination).write_bytes(b"race winner")
            raise FileExistsError(destination)
        with mock.patch.object(oci_archive.os, "link", side_effect=collision):
            with self.assertRaises(FileExistsError):
                oci_archive.convert(self.source, self.destination, REFERENCE)
        self.assertEqual(self.destination.read_bytes(), b"race winner")
        self.assertEqual(list(self.directory.glob(".oci-convert-*")), [])


class OciVerificationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.source, self.archive = self.directory / "source.tar", self.directory / "image.oci.tar"
        entries, _, _, _ = docker_entries()
        save(self.source, entries)
        self.original = oci_archive.convert(self.source, self.archive, REFERENCE)
        self.original_bytes = self.archive.read_bytes()
        self.restore()

    def restore(self):
        self.archive.write_bytes(self.original_bytes)
        self.evidence = copy.deepcopy(self.original)

    def entries(self):
        with tarfile.open(self.archive, mode="r:") as archive:
            return [(member.name, archive.extractfile(member).read())
                    for member in archive if member.isfile()]

    def repack(self, entries):
        save(self.archive, entries)
        self.evidence["archive_sha256"] = hashlib.sha256(self.archive.read_bytes()).hexdigest()

    def edit_manifest(self, edit):
        entries = dict(self.entries())
        index = json.loads(entries["index.json"])
        descriptor = index["manifests"][0]
        old_name = "blobs/" + descriptor["digest"].replace(":", "/")
        manifest = json.loads(entries.pop(old_name))
        edit(manifest)
        data = oci_archive._canonical(manifest)
        descriptor.update(digest=sha(data), size=len(data))
        entries["blobs/" + sha(data).replace(":", "/")] = data
        entries["index.json"] = oci_archive._canonical(index)
        self.evidence["manifest_digest"] = sha(data)
        self.repack(list(entries.items()))

    def test_verifies_real_converter_output_and_streams_layers_without_metadata_copy(self):
        layer_name = "blobs/" + self.evidence["layer_digests"][0].replace(":", "/")
        original = oci_archive._metadata
        def metadata(archive, member):
            self.assertNotEqual(member.name, layer_name, "layer must be streamed, not retained as metadata")
            return original(archive, member)
        with mock.patch.object(oci_archive, "_metadata", side_effect=metadata):
            self.assertEqual(oci_archive.verify_oci_archive(self.archive, {**self.evidence, "binary_sha256": "unused report field"}), self.original)
        self.assertEqual(self.archive.read_bytes(), self.original_bytes)

    def test_gzip_distribution_digest_is_verified_without_layer_decompression(self):
        entries, _, _, _ = docker_entries(compressed=True)
        save(self.source, entries)
        self.archive.unlink()
        result = oci_archive.convert(self.source, self.archive, REFERENCE)
        with mock.patch.object(oci_archive.gzip, "open", side_effect=AssertionError("verifier must not decompress layers")):
            self.assertEqual(oci_archive.verify_oci_archive(self.archive, result), result)

    def test_all_five_evidence_fields_bind_actual_archive_identity(self):
        mutations = {"archive_sha256": "0" * 64, "manifest_digest": "sha256:" + "0" * 64,
                     "config_digest": "sha256:" + "0" * 64, "layer_digests": ["sha256:" + "0" * 64],
                     "reference": "other:tag"}
        for key, value in mutations.items():
            with self.subTest(key=key), self.assertRaises(oci_archive.OciArchiveError):
                oci_archive.verify_oci_archive(self.archive, {**self.evidence, key: value})

    def test_tampered_config_and_layer_fail_even_when_whole_archive_hash_is_updated(self):
        for digest in (self.original["config_digest"], self.original["layer_digests"][0]):
            self.restore()
            entries = dict(self.entries())
            name = "blobs/" + digest.replace(":", "/")
            entries[name] = bytes([entries[name][0] ^ 1]) + entries[name][1:]
            self.repack(list(entries.items()))
            with self.subTest(digest=digest), self.assertRaisesRegex(oci_archive.OciArchiveError, "blob digest mismatch"):
                oci_archive.verify_oci_archive(self.archive, self.evidence)

    def test_outer_inventory_rejects_extras_duplicates_links_and_traversal(self):
        link = tarfile.TarInfo("linked")
        link.type, link.linkname = tarfile.SYMTYPE, "index.json"
        cases = [[("unrelated", b"x")], [("blobs/sha256/" + "0" * 64, b"unreferenced")],
                 [("../escape", b"x")], [(link, None)]]
        for extra in cases:
            self.restore()
            self.repack([*self.entries(), *extra])
            with self.subTest(extra=extra), self.assertRaises(oci_archive.OciArchiveError):
                oci_archive.verify_oci_archive(self.archive, self.evidence)
        self.restore()
        entries = self.entries()
        self.repack([*entries, entries[0]])
        with self.assertRaisesRegex(oci_archive.OciArchiveError, "duplicate outer"):
            oci_archive.verify_oci_archive(self.archive, self.evidence)

    def test_index_requires_single_linux_amd64_manifest_exact_reference_and_schema(self):
        def two_images(index):
            index["manifests"].append(index["manifests"][0])
        mutations = (two_images, lambda index: index["manifests"][0]["platform"].update(architecture="arm64"),
                     lambda index: index["manifests"][0]["annotations"].update({"org.opencontainers.image.ref.name": "other:tag"}),
                     lambda index: index.update(schemaVersion=True),
                     lambda index: index["manifests"][0].update(size=1))
        for mutate in mutations:
            self.restore()
            entries = dict(self.entries())
            index = json.loads(entries["index.json"])
            mutate(index)
            entries["index.json"] = oci_archive._canonical(index)
            self.repack(list(entries.items()))
            with self.subTest(mutation=mutate), self.assertRaises(oci_archive.OciArchiveError):
                oci_archive.verify_oci_archive(self.archive, self.evidence)

    def test_manifest_descriptor_sizes_media_types_and_inventory_are_checked(self):
        mutations = (lambda manifest: manifest["layers"][0].update(size=1),
                     lambda manifest: manifest["layers"][0].update(mediaType="application/untrusted"),
                     lambda manifest: manifest["config"].update(mediaType=oci_archive.OCI_LAYER),
                     lambda manifest: manifest.update(layers=[]),
                     lambda manifest: manifest["layers"][0].update(urls=["https://untrusted.invalid/blob"]))
        for mutate in mutations:
            self.restore()
            self.edit_manifest(mutate)
            with self.subTest(mutation=mutate), self.assertRaises(oci_archive.OciArchiveError):
                oci_archive.verify_oci_archive(self.archive, self.evidence)

    def test_forged_tar_metadata_and_non_tar_archive_are_rejected_after_checksum_match(self):
        member = tarfile.TarInfo("PaxHeader")
        member.type, member.size = tarfile.XHDTYPE, 1 << 40
        cases = [b"not an OCI archive", member.tobuf(format=tarfile.GNU_FORMAT) + b"\0" * 1024]
        for data in cases:
            self.archive.write_bytes(data)
            evidence = {**self.original, "archive_sha256": hashlib.sha256(data).hexdigest()}
            with self.subTest(size=len(data)), self.assertRaises(oci_archive.OciArchiveError):
                oci_archive.verify_oci_archive(self.archive, evidence)


if __name__ == "__main__":
    unittest.main()
