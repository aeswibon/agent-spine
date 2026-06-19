#!/usr/bin/env bash
# Extract a Keep a Changelog section for a release tag.
# Usage: changelog-extract.sh v0.3.0 [CHANGELOG.md]
set -euo pipefail

TAG="${1:?usage: changelog-extract.sh <tag> [file]>}"
FILE="${2:-CHANGELOG.md}"
VERSION="${TAG#v}"

if [[ ! -f "$FILE" ]]; then
  echo "Changelog file not found: $FILE" >&2
  exit 1
fi

awk -v version="$VERSION" '
  $0 ~ "^## \\[" version "\\]" { print; capture=1; next }
  $0 ~ "^## \\[" && capture { exit }
  capture { print }
' "$FILE"
