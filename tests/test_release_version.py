"""Tests for release tag and manifest version consistency."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

MODULE_PATH = Path(__file__).parents[1] / "scripts" / "check_release_version.py"
SPEC = importlib.util.spec_from_file_location("check_release_version", MODULE_PATH)
assert SPEC and SPEC.loader
release_version = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = release_version
SPEC.loader.exec_module(release_version)


class ReleaseVersionTests(unittest.TestCase):
    """Prevent publishing a tag with mismatched integration metadata."""

    def _manifest(self, version: object) -> Path:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "manifest.json"
        path.write_text(json.dumps({"version": version}), encoding="utf-8")
        return path

    def test_stable_version_matches(self) -> None:
        self.assertEqual(
            release_version.check_release_version("v0.6.35", self._manifest("0.6.35")),
            "0.6.35",
        )

    def test_beta_version_matches(self) -> None:
        self.assertEqual(
            release_version.check_release_version(
                "v0.7.0-beta.1", self._manifest("0.7.0-beta.1")
            ),
            "0.7.0-beta.1",
        )

    def test_mismatch_fails(self) -> None:
        with self.assertRaisesRegex(ValueError, "does not match"):
            release_version.check_release_version(
                "v0.7.0-beta.2", self._manifest("0.7.0-beta.1")
            )

    def test_tag_requires_v_prefix(self) -> None:
        with self.assertRaisesRegex(ValueError, "must start"):
            release_version.check_release_version("0.7.0", self._manifest("0.7.0"))
