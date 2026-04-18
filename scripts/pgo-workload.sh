#!/usr/bin/env bash
# Project:   dfe-receiver
# File:      scripts/pgo-workload.sh
# Purpose:   PGO workload orchestrator — spins up Kafka + receiver, drives load
# Language:  Bash
#
# License:   FSL-1.1-ALv2
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# Usage:
#   scripts/pgo-workload.sh <path-to-dfe-receiver-binary>
#
# Environment variables (all optional):
#   PGO_WORKLOAD_DURATION_SECS  Duration of load (default 300)
#   PGO_WORKLOAD_KAFKA_IMAGE    Override Kafka image (default apache/kafka:3.8.0)
#   PGO_WORKLOAD_KEEP           Set to 1 to keep Kafka + receiver on exit (debug)
#
# Preconditions:
#   - Docker daemon running + user has access
#   - The binary passed in $1 must be built with --features jemalloc (or similar
#     instrumented build); it does NOT need the pgo-driver feature
#   - The pgo-driver binary must exist at target/release/pgo-driver OR at
#     $PGO_DRIVER_PATH
#
# Behaviour:
#   - Starts a single-node Kafka (KRaft mode) via docker run
#   - Writes an ephemeral config enabling all listeners on fixed ports
#   - Starts the passed-in receiver binary in background
#   - Waits for /health/ready
#   - Runs pgo-driver for the configured duration
#   - Cleans up (traps EXIT): kills receiver, stops + removes Kafka

set -euo pipefail

# ----------------------------------------------------------------------------
# Args + env
# ----------------------------------------------------------------------------

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <path-to-dfe-receiver-binary>" >&2
    exit 1
fi

RECEIVER_BIN="$1"
if [[ ! -x "$RECEIVER_BIN" ]]; then
    echo "error: $RECEIVER_BIN is not executable" >&2
    exit 1
fi

DURATION="${PGO_WORKLOAD_DURATION_SECS:-300}"
KAFKA_IMAGE="${PGO_WORKLOAD_KAFKA_IMAGE:-apache/kafka:3.8.0}"
KEEP="${PGO_WORKLOAD_KEEP:-0}"

# Floor of 60s — shorter workloads produce bad PGO profiles
if [[ "$DURATION" -lt 60 ]]; then
    echo "error: PGO_WORKLOAD_DURATION_SECS must be >= 60 (got $DURATION)" >&2
    echo "  short workloads produce NEGATIVE PGO gains by biasing the" >&2
    echo "  compiler toward startup paths instead of hot paths" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Locate pgo-driver binary (built as a feature-gated [[bin]])
PGO_DRIVER_PATH="${PGO_DRIVER_PATH:-}"
if [[ -z "$PGO_DRIVER_PATH" ]]; then
    for candidate in \
        "$PROJECT_ROOT/target/release/pgo-driver" \
        "$PROJECT_ROOT/target/debug/pgo-driver"; do
        if [[ -x "$candidate" ]]; then
            PGO_DRIVER_PATH="$candidate"
            break
        fi
    done
fi
if [[ -z "$PGO_DRIVER_PATH" || ! -x "$PGO_DRIVER_PATH" ]]; then
    echo "error: pgo-driver binary not found. Build with:" >&2
    echo "  cargo build --release --features pgo-driver --bin pgo-driver" >&2
    exit 1
fi

# ----------------------------------------------------------------------------
# Cleanup
# ----------------------------------------------------------------------------

RECEIVER_PID=""
KAFKA_CID=""
CONFIG_DIR=""

cleanup() {
    local rc=$?
    if [[ "$KEEP" == "1" ]]; then
        echo "PGO_WORKLOAD_KEEP=1 — skipping cleanup" >&2
        echo "  receiver PID: $RECEIVER_PID" >&2
        echo "  kafka CID:    $KAFKA_CID" >&2
        echo "  config dir:   $CONFIG_DIR" >&2
        return $rc
    fi
    echo "pgo-workload: cleanup" >&2
    if [[ -n "$RECEIVER_PID" ]] && kill -0 "$RECEIVER_PID" 2>/dev/null; then
        kill -TERM "$RECEIVER_PID" 2>/dev/null || true
        # Give the receiver up to 10s to flush + exit cleanly
        for _ in 1 2 3 4 5 6 7 8 9 10; do
            if ! kill -0 "$RECEIVER_PID" 2>/dev/null; then
                break
            fi
            sleep 1
        done
        kill -KILL "$RECEIVER_PID" 2>/dev/null || true
    fi
    if [[ -n "$KAFKA_CID" ]]; then
        docker rm -f "$KAFKA_CID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$CONFIG_DIR" && -d "$CONFIG_DIR" ]]; then
        rm -rf "$CONFIG_DIR"
    fi
    exit $rc
}
trap cleanup EXIT INT TERM

# ----------------------------------------------------------------------------
# Start Kafka (KRaft mode, single-node, auto-create topics)
# ----------------------------------------------------------------------------

echo "pgo-workload: starting Kafka ($KAFKA_IMAGE)"
KAFKA_CID=$(docker run -d --rm \
    -p 19092:9092 \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS='PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093' \
    -e KAFKA_ADVERTISED_LISTENERS='PLAINTEXT://localhost:19092' \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP='CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT' \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS='1@localhost:9093' \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
    -e KAFKA_NUM_PARTITIONS=3 \
    -e KAFKA_DEFAULT_REPLICATION_FACTOR=1 \
    -e CLUSTER_ID="$(printf '%s' "pgo$(date +%s)$$" | base64 | head -c 22)" \
    "$KAFKA_IMAGE")

echo "pgo-workload: Kafka container: $KAFKA_CID"

# Wait for Kafka to accept connections on the host-mapped port.
# We can't `docker exec kafka-topics.sh` here because the broker's
# advertised listener is `localhost:19092` (the host-side mapping) and
# that port doesn't exist inside the container.
for attempt in $(seq 1 30); do
    if (echo > /dev/tcp/127.0.0.1/19092) 2>/dev/null; then
        # TCP accept — give the broker a beat to finish RAFT bootstrap
        sleep 2
        echo "pgo-workload: Kafka ready (attempt $attempt)"
        break
    fi
    if [[ $attempt -eq 30 ]]; then
        echo "error: Kafka did not become ready in 60s" >&2
        docker logs --tail 50 "$KAFKA_CID" >&2
        exit 1
    fi
    sleep 2
done

# ----------------------------------------------------------------------------
# Write ephemeral config
# ----------------------------------------------------------------------------

CONFIG_DIR=$(mktemp -d -t pgo-workload-XXXXXX)
CONFIG_FILE="$CONFIG_DIR/config.yaml"

cat > "$CONFIG_FILE" <<'YAML'
server:
  bind_address: "127.0.0.1:8080"
  max_body_size: 20971520
  request_timeout_ms: 30000
  max_concurrent_requests: 10000
  tls:
    enabled: false
  auth:
    mode: "none"
    include_common_header: false

grpc:
  enabled: false

otlp:
  enabled: true
  grpc_bind_address: "127.0.0.1:4317"
  http_bind_address: "127.0.0.1:4318"
  mode: "generic"
  tls:
    enabled: false
  auth:
    mode: "none"

prometheus_rw:
  enabled: true
  bind_address: "127.0.0.1:9091"
  mode: "native"
  max_body_size: 10485760
  request_timeout_ms: 30000
  auth:
    mode: "none"

splunk_hec:
  enabled: true
  bind_address: "127.0.0.1:8088"
  auth:
    mode: "none"

syslog:
  enabled: true
  # Unprivileged ports — port 514 requires root / CAP_NET_BIND_SERVICE
  # which we don't have in a typical local/CI workload context.
  udp_bind_address: "127.0.0.1:5514"
  tcp_bind_address: "127.0.0.1:5515"
  tls_bind_address: "127.0.0.1:6514"
  max_message_size: 65536
  tls:
    enabled: false
  auth:
    mode: "none"

lumberjack:
  enabled: false

fluent:
  enabled: false

gelf:
  enabled: false

kafka:
  brokers:
    - "localhost:19092"
  client_id: "pgo-workload"

loader:
  transport: "kafka"

destinations:
  default: "kafka"

routing:
  default_source: "pgo"
  topic_suffix: "_land"

buffer:
  memory_limit: 0
  pressure_threshold: 0.8

metrics:
  enabled: true
  address: "127.0.0.1:9090"

log:
  format: "json"
  level: "warn"
YAML

# ----------------------------------------------------------------------------
# Start receiver
# ----------------------------------------------------------------------------

echo "pgo-workload: starting receiver: $RECEIVER_BIN"
echo "pgo-workload: config: $CONFIG_FILE"

# PGO profiles go here by default with cargo-pgo
export LLVM_PROFILE_FILE="${LLVM_PROFILE_FILE:-$PROJECT_ROOT/target/pgo-profiles/pgo-%p_%m.profraw}"
mkdir -p "$(dirname "$LLVM_PROFILE_FILE")"

"$RECEIVER_BIN" --config "$CONFIG_FILE" \
    >"$CONFIG_DIR/receiver.log" 2>&1 &
RECEIVER_PID=$!
echo "pgo-workload: receiver PID: $RECEIVER_PID"

# Wait for readiness
for attempt in $(seq 1 60); do
    if ! kill -0 "$RECEIVER_PID" 2>/dev/null; then
        echo "error: receiver died during startup" >&2
        tail -50 "$CONFIG_DIR/receiver.log" >&2
        exit 1
    fi
    if curl -sf -o /dev/null --max-time 1 "http://127.0.0.1:9090/health/ready" \
        || curl -sf -o /dev/null --max-time 1 "http://127.0.0.1:8080/health/ready"; then
        echo "pgo-workload: receiver ready (attempt $attempt)"
        break
    fi
    if [[ $attempt -eq 60 ]]; then
        echo "error: receiver did not become ready in 60s" >&2
        tail -50 "$CONFIG_DIR/receiver.log" >&2
        exit 1
    fi
    sleep 1
done

# Extra settle time so Kafka connection is fully established before load
sleep 2

# ----------------------------------------------------------------------------
# Run load driver
# ----------------------------------------------------------------------------

echo "pgo-workload: driving load for ${DURATION}s via $PGO_DRIVER_PATH"

PGO_DRIVER_DURATION_SECS="$DURATION" \
PGO_DRIVER_HTTP_URL="http://127.0.0.1:8080/" \
PGO_DRIVER_PROM_RW_URL="http://127.0.0.1:9091/api/v1/write" \
PGO_DRIVER_HEC_URL="http://127.0.0.1:8088/services/collector/event" \
PGO_DRIVER_OTLP_HTTP_URL="http://127.0.0.1:4318/v1/logs" \
PGO_DRIVER_SYSLOG_UDP="127.0.0.1:5514" \
PGO_DRIVER_SYSLOG_TCP="127.0.0.1:5515" \
    "$PGO_DRIVER_PATH"

echo "pgo-workload: driver complete"

# Give the receiver a moment to flush profile data to disk on normal shutdown
sleep 3

echo "pgo-workload: done (receiver logs: $CONFIG_DIR/receiver.log)"
# pgo-workload validated locally 2026-04-18: 879 rps, 0 errors, all 6 protocols clean
# retrigger on hyperi-ci v1.9.2 published to PyPI
