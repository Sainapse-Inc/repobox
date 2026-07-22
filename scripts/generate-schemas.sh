#!/usr/bin/env bash
set -euo pipefail

repository_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
binary=${1:-"$repository_root/target/debug/repobox"}

if [[ ! -x "$binary" ]]; then
  cargo build --manifest-path "$repository_root/Cargo.toml" --package repobox
fi
command -v jq >/dev/null || {
  echo "jq is required to generate Repobox schema snapshots" >&2
  exit 1
}

schema_tmp=$(mktemp -d)
trap 'rm -rf "$schema_tmp"' EXIT

"$binary" agent-context --schemas --json --no-input > "$schema_tmp/context.json"

for name in config success error stream mutation dry_run; do
  output_name=${name//_/-}-v1.json
  jq --sort-keys ".data.schemas.$name" "$schema_tmp/context.json" > "$schema_tmp/$output_name"
  install -m 0644 "$schema_tmp/$output_name" "$repository_root/docs/schemas/$output_name"
done
