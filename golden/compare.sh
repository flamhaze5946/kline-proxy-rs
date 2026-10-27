#!/usr/bin/env bash
# Compare two artifact trees recorded by run.sh (byte-exact; see harness/src/compare.rs).
#   golden/compare.sh <expected_dir> <actual_dir> <checkout used to build the harness>
set -euo pipefail
[ $# -eq 3 ] || { echo "usage: $0 <expected> <actual> <checkout>" >&2; exit 2; }
exec "$(cd "$3" && pwd)/target/kp-golden/target/debug/kp-golden" compare "$1" "$2"
