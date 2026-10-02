#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
cargo fmt --all --check
cargo test --workspace --locked
python3 -m unittest discover -s scripts -p 'test_*.py'
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo build --release --workspace --bins --locked
