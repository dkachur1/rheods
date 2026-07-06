#!/usr/bin/env bash
# Fetches the real upstream `durable-streams` crate source from crates.io and
# extracts it into vendor/. This is a binary crate (src/main.rs, no lib.rs) —
# there is no `repository` we can `git clone`; the Cargo.toml's `repository`
# field points at github.com/durable-streams/durable-streams, which does NOT
# contain this server's source (verified: that repo only has a Rust client and
# a TypeScript reference server). The crates.io tarball is the only place the
# actual source lives, which is exactly how `cargo` itself would fetch it as a
# dependency — same mechanism, just extracted for us to read/patch instead of
# compiled as a dependency.
set -euo pipefail

VERSION="${1:-0.1.2}"
SHA256="786189b7260fd00fa877c067f12b94fc7a02bd932d28fd8ec0f4a48087702ad1"
URL="https://static.crates.io/crates/durable-streams/durable-streams-${VERSION}.crate"

cd "$(dirname "$0")/.."
rm -rf vendor/durable-streams-"${VERSION}"
mkdir -p vendor
curl -sL -o "/tmp/durable-streams-${VERSION}.crate" "$URL"

if [ "$VERSION" = "0.1.2" ]; then
  echo "${SHA256}  /tmp/durable-streams-${VERSION}.crate" | shasum -a 256 -c -
else
  echo "warning: no pinned checksum for version ${VERSION} — verify manually" >&2
fi

tar xzf "/tmp/durable-streams-${VERSION}.crate" -C vendor
echo "vendored into vendor/durable-streams-${VERSION}/"
echo "next: see docs/integration-points.md, then patches/README.md"
