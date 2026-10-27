#!/usr/bin/env bash
# Build the closed-mode golden harness against a kline-proxy-rs checkout and record artifacts.
#   golden/run.sh <kline-proxy-rs checkout> <out_dir> [scenario ...]
# The harness source is copied to <checkout>/target/kp-golden/harness (its own cargo workspace,
# path dependencies on <checkout>/crates/*, Cargo.lock copied from the checkout) and built with
# CARGO_TARGET_DIR=<checkout>/target/kp-golden/target. No tracked file of the checkout changes.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ $# -ge 2 ] || { echo "usage: $0 <checkout> <out_dir> [scenario ...]" >&2; exit 2; }
checkout="$(cd "$1" && pwd)"
out="$2"
shift 2
[ -f "$checkout/crates/kline-runtime/Cargo.toml" ] || { echo "not a kline-proxy-rs checkout: $checkout" >&2; exit 2; }
work="$checkout/target/kp-golden"
rm -rf "$work/harness"
mkdir -p "$work/harness"
cp -R "$here/harness/src" "$work/harness/src"
sed "s#@CHECKOUT@#$checkout#g" "$here/harness/Cargo.toml.in" > "$work/harness/Cargo.toml"
cp "$checkout/Cargo.lock" "$work/harness/Cargo.lock"
echo "== checkout $(git -C "$checkout" rev-parse --short HEAD 2>/dev/null || echo '?') $(git -C "$checkout" status --porcelain 2>/dev/null | wc -l | tr -d ' ') dirty; $(rustc --version)" >&2
CARGO_TARGET_DIR="$work/target" cargo build --offline --quiet --manifest-path "$work/harness/Cargo.toml"
bin="$work/target/debug/kp-golden"
ls -l "$bin" >&2
mkdir -p "$out"
out="$(cd "$out" && pwd)"
if [ $# -gt 0 ]; then scenarios="$*"; else scenarios="$("$bin" list)"; fi
for s in $scenarios; do
  start=$(date +%s)
  if ! "$bin" run "$s" "$out/$s" "$work/work/$s"; then
    echo "FAILED scenario $s" >&2
    exit 1
  fi
  echo "ok $s ($(( $(date +%s) - start ))s)" >&2
done
"$bin" hash "$out" > "$out/MANIFEST.blake3"
tail -1 "$out/MANIFEST.blake3"
