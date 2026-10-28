import importlib.util
from pathlib import Path
import tempfile
import unittest

MODULE = Path(__file__).resolve().parents[1] / "check-release-version.py"
SPEC = importlib.util.spec_from_file_location("release_version", MODULE)
release_version = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release_version)


class ReleaseVersionTests(unittest.TestCase):
    def test_tag_matches_embedded_version(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "Cargo.toml"
            manifest.write_text('[package]\nversion = "1.0.0-rc.1"\n', encoding="utf-8")
            self.assertEqual(release_version.validate("v1.0.0-rc.1", manifest), "1.0.0-rc.1")
            for tag in ("v1.0.0", "v0.1.0", "1.0.0-rc.1", "v1", "v1.0.0/extra"):
                with self.subTest(tag=tag), self.assertRaises(ValueError):
                    release_version.validate(tag, manifest)


if __name__ == "__main__":
    unittest.main()
