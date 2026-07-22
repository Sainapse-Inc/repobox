#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 4 ]]; then
  echo "usage: $0 DIRECTORY TAG COMMIT VERSION" >&2
  exit 2
fi

directory="$1"
tag="$2"
commit="$3"
version="$4"
expected=(
  "repobox-$tag-x86_64-unknown-linux-gnu.tar.gz"
  "repobox-$tag-aarch64-unknown-linux-gnu.tar.gz"
  "repobox-$tag-x86_64-apple-darwin.tar.gz"
  "repobox-$tag-aarch64-apple-darwin.tar.gz"
)
metadata=(SHA256SUMS RELEASE-MANIFEST.json)

if [[ ! -d "$directory" ]]; then
  echo "release asset directory does not exist: $directory" >&2
  exit 1
fi

shopt -s nullglob
entries=("$directory"/*)
if [[ "${#entries[@]}" -ne 6 ]]; then
  echo "expected exactly six release assets, found ${#entries[@]}" >&2
  exit 1
fi
for entry in "${entries[@]}"; do
  if [[ ! -f "$entry" ]]; then
    echo "unexpected non-file release asset: $entry" >&2
    exit 1
  fi
done
for filename in "${expected[@]}" "${metadata[@]}"; do
  if [[ ! -f "$directory/$filename" ]]; then
    echo "missing release asset: $filename" >&2
    exit 1
  fi
done

jq -e --arg tag "$tag" --arg commit "$commit" --arg version "$version" '.schema_version == 1
    and .tag == $tag
    and .commit == $commit
    and .version == $version
    and (.sha256sums | type == "array" and length == 4)' "$directory/RELEASE-MANIFEST.json" >/dev/null

manifest_checksums="$(mktemp)"
trap 'rm -f "$manifest_checksums"' EXIT
jq -r '.sha256sums[]' "$directory/RELEASE-MANIFEST.json" > "$manifest_checksums"
cmp --silent "$directory/SHA256SUMS" "$manifest_checksums"

for archive in "${expected[@]}"; do
  count="$(awk -v filename="$archive" '$2 == filename { count++ } END { print count + 0 }' "$directory/SHA256SUMS")"
  checksum="$(awk -v filename="$archive" '$2 == filename { print $1 }' "$directory/SHA256SUMS")"
  if [[ "$count" -ne 1 || ! "$checksum" =~ ^[0-9a-f]{64}$ ]]; then
    echo "expected one valid checksum for $archive" >&2
    exit 1
  fi
  tar -tzf "$directory/$archive" >/dev/null
done

if command -v sha256sum >/dev/null 2>&1; then
  (cd "$directory" && sha256sum --check SHA256SUMS)
else
  (cd "$directory" && shasum -a 256 --check SHA256SUMS)
fi
