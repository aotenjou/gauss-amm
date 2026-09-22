#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="${LLM4SQLGEN_RUNTIME_DIR:-$ROOT_DIR/runtime}"
export GAUSSHOME="${GAUSSHOME:-}"
GSBENCH_ARCHIVE="${GSBENCH_ARCHIVE:-}"
GSBENCH_RELEASE_DIR="${GSBENCH_RELEASE_DIR:-}"
DB_USER="${DB_USER:-$(id -un)}"
DB_PASSWORD="${DB_PASSWORD:-}"
MIN_FREE_DISK_PERCENT="${MIN_FREE_DISK_PERCENT:-10}"
PORT_MIN="${PORT_MIN:-15433}"
PORT_MAX="${PORT_MAX:-15460}"
DATA_DIR="$RUNTIME_DIR/openGauss-data"
SOCKET_DIR="$RUNTIME_DIR/socket"
SERVER_LOG="$RUNTIME_DIR/openGauss-server.log"
GSBENCH_DIR="$RUNTIME_DIR/gsbench-v1.1.9-linux-amd64"

export PATH="$GAUSSHOME/bin:$PATH"
COMPAT_LIB_DIR="${GAUSS_COMPAT_LIB_DIR:-}"
PRIVATE_COMPAT_DIR="$RUNTIME_DIR/compat"
OPENSSL_RPM_LIB_DIR="${OPENSSL_RPM_LIB_DIR:-$RUNTIME_DIR/compat-rpm/root2/usr/lib64}"

: "${GAUSSHOME:?set GAUSSHOME to the openGauss installation directory}"
: "${GSBENCH_ARCHIVE:?set GSBENCH_ARCHIVE to the gsbench release archive}"
: "${GSBENCH_RELEASE_DIR:?set GSBENCH_RELEASE_DIR to the gsbench release directory}"
: "${DB_PASSWORD:?set DB_PASSWORD for the temporary benchmark instance}"
: "${COMPAT_LIB_DIR:?set GAUSS_COMPAT_LIB_DIR to the compatibility library directory}"

for command_name in gaussdb gs_ctl gs_initdb gsql sha256sum tar; do
  command -v "$command_name" >/dev/null || { echo "missing command: $command_name" >&2; exit 1; }
done
test -f "$GSBENCH_ARCHIVE" || { echo "gsbench archive not found: $GSBENCH_ARCHIVE" >&2; exit 1; }
test -x "$GAUSSHOME/bin/gaussdb" || { echo "gaussdb not found under $GAUSSHOME" >&2; exit 1; }

if [[ -e "$DATA_DIR/postmaster.pid" ]]; then
  # A failed gs_ctl invocation can leave an empty or stale PID file behind.
  # Refuse only when the recorded process is still alive.
  running_pid=""
  if [[ -s "$DATA_DIR/postmaster.pid" ]]; then
    running_pid="$(sed -n '1p' "$DATA_DIR/postmaster.pid" 2>/dev/null || true)"
  fi
  if [[ "$running_pid" =~ ^[0-9]+$ ]] && kill -0 "$running_pid" 2>/dev/null; then
    echo "refusing to reuse running data directory: $DATA_DIR (pid $running_pid)" >&2
    exit 1
  fi
  echo "removing stale postmaster PID files from $DATA_DIR"
  rm -f "$DATA_DIR/postmaster.pid" "$DATA_DIR/postmaster.pid.lock" "$DATA_DIR/pg_ctl.lock"
fi
if [[ -e "$DATA_DIR/PG_VERSION" ]]; then
  echo "removing incomplete unstarted data directory: $DATA_DIR"
  rm -rf "$DATA_DIR"
fi
mkdir -p "$RUNTIME_DIR" "$SOCKET_DIR"
chmod 700 "$RUNTIME_DIR" "$SOCKET_DIR"
mkdir -p "$PRIVATE_COMPAT_DIR"
for compat_lib in libreadline.so.6 libtinfo.so.5 libncurses.so.5; do
  test -e "$COMPAT_LIB_DIR/$compat_lib" || { echo "missing compatibility library: $COMPAT_LIB_DIR/$compat_lib" >&2; exit 1; }
  cp -L "$COMPAT_LIB_DIR/$compat_lib" "$PRIVATE_COMPAT_DIR/$compat_lib"
done
# libeSDKOBS in the 5.0.2 package retains OpenSSL 1.0 SONAMEs while gaussdb
# itself uses OpenSSL 1.1. The host compatibility tree supplies the former.
for ssl_pair in "libssl.so.1.0.2k libssl.so.10" "libcrypto.so.1.0.2k libcrypto.so.10"; do
  set -- $ssl_pair
  test -e "$OPENSSL_RPM_LIB_DIR/$1" || { echo "missing compatibility library: $OPENSSL_RPM_LIB_DIR/$1" >&2; exit 1; }
  cp -L "$OPENSSL_RPM_LIB_DIR/$1" "$PRIVATE_COMPAT_DIR/$2"
done
export LD_LIBRARY_PATH="$GAUSSHOME/lib:$GAUSSHOME/lib/postgresql:$GAUSSHOME/lib/krb5:$PRIVATE_COMPAT_DIR:${LD_LIBRARY_PATH:-}"
export GSBENCH_PASSWORD="$DB_PASSWORD"

PORT="$(python3 - "$PORT_MIN" "$PORT_MAX" <<'PY'
import socket, sys
for value in range(int(sys.argv[1]), int(sys.argv[2]) + 1):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", value))
    except OSError:
        sock.close()
        continue
    sock.close()
    print(value)
    break
else:
    raise SystemExit("no free localhost port in requested range")
PY
)"

echo "initializing openGauss 5.0.2 on port $PORT"
PWFILE="$RUNTIME_DIR/.initdb-password.$$"
printf '%s\n' "$DB_PASSWORD" > "$PWFILE"
chmod 600 "$PWFILE"
cleanup_password() { rm -f "$PWFILE"; }
trap cleanup_password EXIT
gs_initdb -D "$DATA_DIR" --nodename=single_node -U "$DB_USER" --pwfile="$PWFILE" >/dev/null
rm -f "$PWFILE"
trap - EXIT
printf '\nhost all all 127.0.0.1/32 trust\n' >> "$DATA_DIR/pg_hba.conf"
gs_ctl -D "$DATA_DIR" -l "$SERVER_LOG" start -o "-p $PORT -h 127.0.0.1 -k $SOCKET_DIR" >/dev/null

cleanup_on_error() {
  if ! gsql -h "$SOCKET_DIR" -p "$PORT" -U "$DB_USER" -d postgres -Atc 'select 1' >/dev/null 2>&1; then
    gs_ctl -D "$DATA_DIR" stop -m fast >/dev/null 2>&1 || true
  fi
}
trap cleanup_on_error ERR

for _ in $(seq 1 60); do
  if gsql -h "$SOCKET_DIR" -p "$PORT" -U "$DB_USER" -d postgres -Atc 'select version()' >/tmp/llm4sqlgen-version.$$ 2>/dev/null; then
    break
  fi
  sleep 1
done
test -s /tmp/llm4sqlgen-version.$$ || { echo "new openGauss instance did not become ready; see $SERVER_LOG" >&2; exit 1; }
VERSION="$(sed -n '1p' /tmp/llm4sqlgen-version.$$)"
rm -f /tmp/llm4sqlgen-version.$$
case "$VERSION" in
  *"openGauss 5.0.2"*) ;;
  *) echo "unexpected server version: $VERSION" >&2; exit 1;;
esac

declare -a SIZES=(1 2 5 10 15 20)
for size in "${SIZES[@]}"; do
  db="llm4sqlgen_s${size}gb"
  gsql -h "$SOCKET_DIR" -p "$PORT" -U "$DB_USER" -d postgres -v ON_ERROR_STOP=1 \
    -c "CREATE DATABASE $db" >/dev/null
done

if [[ ! -x "$GSBENCH_DIR/bin/gsbench" ]]; then
  rm -rf "$GSBENCH_DIR"
  tar -xzf "$GSBENCH_ARCHIVE" -C "$RUNTIME_DIR"
fi
"$GSBENCH_DIR/bin/gsbench" version >/dev/null
(
  cd "$GSBENCH_RELEASE_DIR"
  sha256sum -c SHA256SUMS --ignore-missing >/dev/null
)

for size in "${SIZES[@]}"; do
  db="llm4sqlgen_s${size}gb"
  config="$RUNTIME_DIR/gsbench-${size}gb.cfg"
  sed -e "s/^host = .*/host = 127.0.0.1/" \
      -e "s/^port = .*/port = $PORT/" \
      -e "s/^database = .*/database = $db/" \
      -e "s/^user = .*/user = $DB_USER/" \
      -e 's/^schema = .*/schema = gsbench/' \
      -e 's/^reuse_existing = .*/reuse_existing = false/' \
      -e "s/^min_free_disk_percent = .*/min_free_disk_percent = $MIN_FREE_DISK_PERCENT/" \
      -e "s/^max_size_gb = .*/max_size_gb = $size/" \
      "$GSBENCH_DIR/configs/gsbench.cfg" > "$config"
  chmod 600 "$config"
  echo "initializing gsbench ${size}GB in $db"
  if ! "$GSBENCH_DIR/bin/gsbench" init --config "$config" --size "${size}GB"; then
    # gsbench may lose its long-lived connection while releasing the final
    # advisory locks even though all tables and indexes were committed.
    table_count="$(gsql -h "$SOCKET_DIR" -p "$PORT" -U "$DB_USER" -d "$db" -Atc \
      "select count(*) from information_schema.tables where table_schema='gsbench'" 2>/dev/null || echo 0)"
    if [[ "$table_count" =~ ^[0-9]+$ ]] && (( table_count >= 20 )); then
      echo "gsbench ${size}GB completed data load; ignoring cleanup connection error"
    else
      echo "gsbench ${size}GB failed before completing the gsbench schema" >&2
      exit 1
    fi
  fi
done

python3 - "$RUNTIME_DIR/instance.json" "$PORT" "$DATA_DIR" "$SOCKET_DIR" "$SERVER_LOG" "$GSBENCH_DIR" "$DB_USER" <<'PY'
import json, sys
path, port, data, socket_dir, log, gsbench, user = sys.argv[1:]
json.dump({"port": int(port), "data_dir": data, "socket_dir": socket_dir,
          "server_log": log, "gauss_home": __import__('os').environ["GAUSSHOME"],
          "gsbench_dir": gsbench, "user": user,
          "databases": {str(n): f"llm4sqlgen_s{n}gb" for n in (1, 2, 5, 10, 15, 20)},
          "schema": "gsbench", "version": "openGauss 5.0.2"},
         open(path, "w", encoding="utf-8"), indent=2)
PY
chmod 600 "$RUNTIME_DIR/instance.json"
trap - ERR
echo "instance ready: $RUNTIME_DIR/instance.json"
