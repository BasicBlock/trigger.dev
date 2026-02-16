#!/bin/bash

set -euo pipefail

token="${NODE_AUTH_TOKEN:-${GITHUB_TOKEN:-${GH_TOKEN:-}}}"

if [[ -z "$token" ]]; then
  echo "Missing auth token. Set NODE_AUTH_TOKEN, GITHUB_TOKEN, or GH_TOKEN."
  exit 1
fi

if [[ $(git status --porcelain) ]]; then
  echo "Your git status is not clean. Commit/stash changes before publishing."
  exit 1
fi

npmrc_file=$(mktemp)
cleanup() {
  rm -f "$npmrc_file"
}
trap cleanup EXIT

cat > "$npmrc_file" <<EOF
@basicblock:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${token}
EOF

export npm_config_userconfig="$npmrc_file"
export NODE_AUTH_TOKEN="$token"

echo "Running: pnpm install --frozen-lockfile"
pnpm install --frozen-lockfile

echo "Running: pnpm run generate"
pnpm run generate

echo "Running: pnpm run build --filter \"@basicblock/trigger-*\""
pnpm run build --filter "@basicblock/trigger-*"

echo "Going to run: pnpm exec changeset publish"
read -p "Continue? (y/N): " prompt
if [[ ! $prompt =~ [yY](es)* ]]; then
  echo "Aborted."
  exit 1
fi

pnpm exec changeset publish
