#!/bin/sh
set -eu

# Base-image preparation for the S2 workload harness.
#
# The system under test is the vendored s2 release binary (v0.38.0,
# x86_64-unknown-linux-musl, static-pie) which embeds both the S2 CLI and the
# `s2 lite` self-hostable server. No toolchain is required in the image:
# workloads are POSIX sh scripts that start `s2 lite` and drive it with the
# same binary. Everything is offline — the binary travels in git.

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
S2_BIN="${ROOT}/.workers/vendor/bin/s2-linux-amd64"

if [ ! -f "${S2_BIN}" ]; then
  echo "missing vendored s2 binary at ${S2_BIN}" >&2
  exit 1
fi
chmod +x "${S2_BIN}"

# Stable path for workloads regardless of vendor layout changes.
ln -sf "${S2_BIN}" "${ROOT}/.workers/vendor/bin/s2"

"${S2_BIN}" --version

echo "build.sh: s2 binary staged at .workers/vendor/bin/s2"
