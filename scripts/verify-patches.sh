#!/usr/bin/env bash
# Verifies every patch in patches/ applies cleanly to a pristine vendored
# tree AND that the result compiles. Operates on a throwaway copy so the real
# vendor/ tree is left untouched. Run after editing a patch or bumping the
# vendored version.
set -euo pipefail

cd "$(dirname "$0")/.."
REPO="$PWD"
VERSION="0.1.2"
CRATE="/tmp/durable-streams-${VERSION}.crate"

# Always verify against a FRESH pristine extraction, never the working vendor/
# tree (which may already have patches applied). Fetch the crate if absent.
if [ ! -f "$CRATE" ]; then
  curl -sL -o "$CRATE" \
    "https://static.crates.io/crates/durable-streams/durable-streams-${VERSION}.crate"
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
tar xzf "$CRATE" -C "$WORK"

cd "$WORK/durable-streams-${VERSION}"
for p in "$REPO"/patches/*.patch; do
  [ -e "$p" ] || continue
  echo "=== applying $(basename "$p") ==="
  patch -p1 < "$p"
done

# Exclude from any parent workspace so it builds standalone.
if ! grep -q '^\[workspace\]' Cargo.toml; then
  printf '\n[workspace]\n' >> Cargo.toml
fi

echo "=== building patched tree ==="
cargo build
echo "OK — all patches applied and the patched crate compiles"
