#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
# cargo-zigbuild and Zig must already be installed; target libraries come from rustup.
# Explicit RUSTC avoids selecting Homebrew's different sysroot on macOS.
KLINE_RUSTC="$(rustup which rustc)"
RUSTC="$KLINE_RUSTC" cargo zigbuild --release --locked --target x86_64-unknown-linux-gnu.2.28 -p kline-runtime --bin kline-proxy
