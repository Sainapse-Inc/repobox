#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "usage: $0 DESTINATION" >&2
  exit 2
fi

destination="$1"
version="8.30.1"
archive="gitleaks_${version}_linux_x64.tar.gz"
sha256="551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb"
download_url="https://github.com/gitleaks/gitleaks/releases/download/v${version}/${archive}"
temporary_directory="$(mktemp -d)"
trap 'rm -rf -- "$temporary_directory"' EXIT

curl --fail --location --retry 3 --retry-all-errors \
  --proto '=https' --tlsv1.2 \
  --output "$temporary_directory/$archive" \
  "$download_url"

(
  cd "$temporary_directory"
  printf '%s  %s\n' "$sha256" "$archive" | sha256sum --check -
  tar -xzf "$archive" gitleaks
)

install -m 0755 "$temporary_directory/gitleaks" "$destination"
