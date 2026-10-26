#!/bin/sh
# Stage the kp-1m evidence rehearsal for the Tokyo test VPS and PRINT the commands to install it.
# This script never connects anywhere: no ssh, scp, systemctl or Binance access. It only
#   * optionally builds the linux binary locally (--build: scripts/build-linux.sh, cargo-zigbuild),
#   * writes a staging directory with binary + config + units + scripts + initial breaker.json,
#   * writes <stage>/commands.sh (install) and <stage>/rollback.sh for an operator to run by hand.
# Usage: rehearsal/install.sh --release s3b-ece092b [--binary PATH | --build] [--out DIR] [--host root@45.32.255.251]
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(dirname "$HERE")
RELEASE=""
BINARY=""
BUILD=0
OUT="$HERE/stage"
HOST="root@45.32.255.251"
while [ $# -gt 0 ]; do
  case "$1" in
    --release) RELEASE=$2; shift 2 ;;
    --binary) BINARY=$2; shift 2 ;;
    --build) BUILD=1; shift ;;
    --out) OUT=$2; shift 2 ;;
    --host) HOST=$2; shift 2 ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done
[ -n "$RELEASE" ] || { echo "--release is required (e.g. s3b-$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown))" >&2; exit 2; }
case "$RELEASE" in *[!A-Za-z0-9._-]*) echo "release name must be [A-Za-z0-9._-]" >&2; exit 2 ;; esac
if [ "$BUILD" = 1 ]; then
  sh "$ROOT/scripts/build-linux.sh"
  BINARY="$ROOT/target/x86_64-unknown-linux-gnu/release/kline-proxy"
fi
STAGE="$OUT/kp-1m-$RELEASE"
rm -rf "$STAGE"
mkdir -p "$STAGE/units" "$STAGE/rehearsal"
if [ -n "$BINARY" ]; then
  cp "$BINARY" "$STAGE/kline-proxy"
  file "$STAGE/kline-proxy" | grep -q 'ELF 64-bit LSB.*x86-64' || { echo "binary is not linux x86-64: $(file "$STAGE/kline-proxy")" >&2; exit 3; }
else
  echo "NOTE: no --binary/--build given; $STAGE/kline-proxy must be added before copying" >&2
fi
cp "$HERE/kp-1m.json" "$STAGE/$RELEASE.json"
cp "$HERE"/systemd/*.service "$STAGE/units/"
for f in kp_lib.py passthrough.py recorder_evidence.py repull.py inject.py load_f16.py summarize_rec.py; do cp "$HERE/$f" "$STAGE/rehearsal/"; done
printf '{"last_conflict_ms":0,"revised_total":0}' > "$STAGE/breaker.json"
GIT=$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo unknown)
DIRTY=$(git -C "$ROOT" status --porcelain -- crates Cargo.toml Cargo.lock 2>/dev/null | wc -l | tr -d ' ')

cat > "$STAGE/commands.sh" <<EOF
#!/bin/sh
# kp-1m evidence rehearsal install, release $RELEASE (source $GIT, crates dirty files: $DIRTY).
# Run BY HAND on the VPS as root, step by step. Preconditions (RUNBOOK.md section 1):
#   * M10c sampler and the 0.4.24 pre-sample have finished (not before 2026-10-27T11:35Z);
#   * nothing listens on 127.0.0.1:1891 / 18980-18982;
#   * same-IP REST budget: kp 1200 + recorder cap 120 (+ anything else still running) <= 1800/min.
# Copy from the workstation first:  scp -r $STAGE $HOST:/tmp/
set -eu
S=/tmp/kp-1m-$RELEASE
[ "\$(date -u +%s)" -ge "\$(date -u -d 2026-10-27T11:35:00Z +%s)" ] || { echo "M10c window not over"; exit 1; }
! ss -ltn | grep -Eq '127.0.0.1:(1891|18980|18981|18982) ' || { echo "port busy"; exit 1; }
cd "\$S" && sha256sum -c SHA256SUMS
id kp1m >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin kp1m
install -d -m 0755 /opt/kp-1m/evidence/$RELEASE /opt/kp-1m/rehearsal /etc/kp-1m/evidence
install -m 0755 "\$S/kline-proxy" /opt/kp-1m/evidence/$RELEASE/kline-proxy
install -m 0644 "\$S/$RELEASE.json" /etc/kp-1m/evidence/$RELEASE.json
install -m 0644 "\$S"/rehearsal/*.py /opt/kp-1m/rehearsal/
/opt/kp-1m/evidence/$RELEASE/kline-proxy --check-config /etc/kp-1m/evidence/$RELEASE.json
# Switch binary and config TOGETHER; remember the previous pair for rollback.sh.
for l in /opt/kp-1m/evidence/current /etc/kp-1m/evidence/current.json; do readlink "\$l" || true; done > /etc/kp-1m/evidence/previous.$RELEASE
ln -sfn /opt/kp-1m/evidence/$RELEASE /opt/kp-1m/evidence/current
ln -sfn /etc/kp-1m/evidence/$RELEASE.json /etc/kp-1m/evidence/current.json
install -d -o kp1m -g kp1m -m 0750 /var/lib/kp-1m-evidence /var/lib/kp-1m-o7
# Initial breaker = deployment decision that WS finals are trusted (design v8 section 5).
[ -e /var/lib/kp-1m-evidence/breaker.json ] || install -o kp1m -g kp1m -m 0640 "\$S/breaker.json" /var/lib/kp-1m-evidence/breaker.json
install -m 0644 "\$S"/units/*.service /etc/systemd/system/
systemctl daemon-reload
systemctl start kp-1m-passthrough.service
systemctl start kp-1m-evidence.service
timeout 300 sh -c 'until curl -fsS http://127.0.0.1:1891/health/ready >/dev/null; do sleep 2; done'
curl -fsS 'http://127.0.0.1:1891/fapi/v1/klines/bulk?interval=1m&closed_only=true&with_evidence=true&limit=3&symbols=BTCUSDT' | head -c 400; echo
systemctl show -p MemoryMax kp-1m-evidence.service
# 24 h recording (RUNBOOK section 3); starts only after the health check above.
systemctl start kp-1m-o7-recorder.service
EOF

cat > "$STAGE/rollback.sh" <<EOF
#!/bin/sh
# Roll back release $RELEASE: binary AND config together (0.4.24 rejects the evidence config,
# deny_unknown_fields). Run by hand on the VPS as root.
set -eu
systemctl stop kp-1m-o7-recorder.service 2>/dev/null || true
systemctl stop kp-1m-evidence.service
P=/etc/kp-1m/evidence/previous.$RELEASE
BIN_PREV=\$(sed -n 1p "\$P"); CFG_PREV=\$(sed -n 2p "\$P")
if [ -n "\$BIN_PREV" ] && [ -n "\$CFG_PREV" ]; then
  "\$BIN_PREV/kline-proxy" --check-config "\$CFG_PREV"
  ln -sfn "\$BIN_PREV" /opt/kp-1m/evidence/current
  ln -sfn "\$CFG_PREV" /etc/kp-1m/evidence/current.json
  systemctl start kp-1m-evidence.service
else
  # First install: there is no previous evidence pair. Leave the unit stopped; the 0.4.24
  # instance keeps its own binary+config (/opt/kp-1m/kline-proxy + /etc/kp-1m/config-cont.json).
  systemctl disable kp-1m-evidence.service 2>/dev/null || true
  echo "no previous evidence release; kp-1m-evidence stopped"
fi
# The state directory keeps 0.4.24-format snapshots; breaker.json/retained.json are ignored by 0.4.24.
EOF
chmod +x "$STAGE/commands.sh" "$STAGE/rollback.sh"
(cd "$STAGE" && find . -type f ! -name SHA256SUMS | sort | xargs shasum -a 256 > SHA256SUMS)
echo "staged $STAGE"
echo "next (by hand): scp -r $STAGE $HOST:/tmp/  then run /tmp/kp-1m-$RELEASE/commands.sh on the VPS"
