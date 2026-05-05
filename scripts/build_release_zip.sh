#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
zip_path="${1:-/home/ubuntu/leapmotor-ha.zip}"

rm -f "$zip_path"
(
  cd "$repo_root/custom_components/leapmotor"
  zip -r "$zip_path" . \
    -x '__pycache__/*' \
    -x '*.pyc' \
    -x 'app_cert.pem' \
    -x 'app_key.pem' \
    -x '*/.DS_Store'
)

python3 - "$zip_path" <<'PY'
import sys
import zipfile

zip_path = sys.argv[1]
with zipfile.ZipFile(zip_path) as archive:
    names = set(archive.namelist())
    required = {"manifest.json", "__init__.py", "api.py"}
    missing = required - names
    forbidden = {
        "custom_components/leapmotor/manifest.json",
        "app_cert.pem",
        "app_key.pem",
    } & names
    if missing:
        raise SystemExit(f"release ZIP missing root files: {sorted(missing)}")
    if forbidden:
        raise SystemExit(f"release ZIP contains forbidden entries: {sorted(forbidden)}")
PY
