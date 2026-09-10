#!/usr/bin/env python3
"""Verify that a release tag matches the Home Assistant manifest version."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


DEFAULT_MANIFEST = Path("custom_components/leapmotor/manifest.json")


def check_release_version(tag: str, manifest_path: Path = DEFAULT_MANIFEST) -> str:
    """Return the matching version or raise ValueError for unsafe release metadata."""
    if not tag.startswith("v") or len(tag) == 1:
        raise ValueError(f"Release tag must start with 'v': {tag!r}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest_version = manifest.get("version")
    if not isinstance(manifest_version, str) or not manifest_version:
        raise ValueError(f"Manifest has no valid version: {manifest_version!r}")

    tag_version = tag[1:]
    if tag_version != manifest_version:
        raise ValueError(
            f"Release tag {tag!r} does not match manifest version {manifest_version!r}"
        )
    return manifest_version


def main() -> int:
    """Run the release version check from the command line."""
    parser = argparse.ArgumentParser()
    parser.add_argument("tag", help="Release tag, including the leading v")
    parser.add_argument("manifest", nargs="?", type=Path, default=DEFAULT_MANIFEST)
    args = parser.parse_args()

    try:
        version = check_release_version(args.tag, args.manifest)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.exit(1, f"release version check failed: {exc}\n")
    print(f"ok release tag v{version} matches manifest")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
