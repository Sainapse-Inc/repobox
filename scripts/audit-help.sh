#!/usr/bin/env bash
set -euo pipefail

repository_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
binary=${1:-"$repository_root/target/debug/repobox"}

if [[ ! -x "$binary" ]]; then
  cargo build --manifest-path "$repository_root/Cargo.toml" --package repobox
fi
command -v jq >/dev/null || {
  echo "jq is required to audit the command manifest" >&2
  exit 1
}

commands=$(
  "$binary" agent-context --json --no-input |
    jq -r '
      def paths($prefix):
        .subcommands[] as $command |
        (($prefix + [$command.name]) | join(" ")),
        ($command | paths($prefix + [$command.name]));
      .data.commands | paths([])
    '
)

missing=()
while IFS= read -r command_path; do
  read -r -a argv <<< "$command_path"
  if ! "$binary" "${argv[@]}" --help | grep -q '^EXAMPLES:'; then
    missing+=("$command_path")
  fi
done <<< "$commands"

if ((${#missing[@]})); then
  printf 'Commands missing concrete examples:\n' >&2
  printf '  %s\n' "${missing[@]}" >&2
  exit 1
fi

printf 'All command help pages include concrete examples.\n'
