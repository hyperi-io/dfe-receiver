#!/usr/bin/env bash
# Project:   dfe-receiver
# File:      scripts/pgo-workload.sh
# Purpose:   PGO workload orchestrator -- spins up Kafka + receiver, drives load
# Language:  Bash
#
# License:   BUSL-1.1
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# Usage:
#   scripts/pgo-workload.sh <path-to-dfe-receiver-binary>
#
# Environment variables (all optional):
#   PGO_WORKLOAD_DURATION_SECS  Duration of load (default 300)
#   PGO_WORKLOAD_KAFKA_IMAGE    Override broker image (default Redpanda; speaks Kafka wire protocol)
#   PGO_WORKLOAD_KEEP           Set to 1 to keep broker + receiver on exit (debug)
#
# Preconditions:
#   - Docker daemon running + user has access
#   - The binary passed in $1 must be built with --features jemalloc (or similar
#     instrumented build); it does NOT need the pgo-driver feature
#   - The pgo-driver binary must exist at target/release/pgo-driver OR at
#     $PGO_DRIVER_PATH
#
# Behaviour:
#   - Starts a single-node Redpanda broker (Kafka wire protocol) via docker run
#   - Writes an ephemeral config: the listeners the driver feeds, source rules
#     on data_stream.dataset as a deployment compiles them, delivery to Kafka
#   - Starts the passed-in receiver binary in background
#   - Waits for /readyz
#   - Runs pgo-driver for the configured duration; it exits non-zero unless
#     every listener took records and the receiver delivered them to Kafka
#   - Fails unless each routed landing topic holds records
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
# The broker equals dfe-infra versions.yaml services.redpanda-version, pinned by
# digest so a rebuilt tag cannot change it. Tag and digest sit on their own
# lines so the org Renovate regex can read them.
# renovate: datasource=docker depName=docker.redpanda.com/redpandadata/redpanda
KAFKA_TAG="v26.2.3"
KAFKA_DIGEST="sha256:9e83cfa99278f30d0133271c26bf670cd69c94ffa6ba0b42830dd0c3bd9dcfd9"
KAFKA_IMAGE="${PGO_WORKLOAD_KAFKA_IMAGE:-docker.redpanda.com/redpandadata/redpanda:${KAFKA_TAG}@${KAFKA_DIGEST}}"
KEEP="${PGO_WORKLOAD_KEEP:-0}"

# Floor of 60s -- shorter workloads produce bad PGO profiles
if [[ "$DURATION" -lt 60 ]]; then
    echo "error: PGO_WORKLOAD_DURATION_SECS must be >= 60 (got $DURATION)" >&2
    echo "  short workloads produce NEGATIVE PGO gains by biasing the" >&2
    echo "  compiler toward startup paths instead of hot paths" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Locate pgo-driver binary (built as a feature-gated [[bin]]).
# On-demand build if missing: hyperi-ci's release build only produces the
# main binary, not the pgo-driver (which requires --features pgo-driver).
# Building here is cheap (~1 min, deps already compiled via cargo cache).
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
    echo "pgo-workload: pgo-driver not found, building..." >&2
    (cd "$PROJECT_ROOT" && cargo build --release --features pgo-driver --bin pgo-driver) \
        || { echo "error: failed to build pgo-driver" >&2; exit 1; }
    PGO_DRIVER_PATH="$PROJECT_ROOT/target/release/pgo-driver"
    if [[ ! -x "$PGO_DRIVER_PATH" ]]; then
        echo "error: pgo-driver still missing after build at $PGO_DRIVER_PATH" >&2
        exit 1
    fi
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
        echo "PGO_WORKLOAD_KEEP=1 -- skipping cleanup" >&2
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

echo "pgo-workload: starting Redpanda ($KAFKA_IMAGE)"
# Redpanda speaks the Kafka wire protocol (app config unchanged) but is a
# single C++/Seastar binary that boots in ~1s and fits a hard 512 MiB cap --
# unlike the Kafka JVM (1.5-2 GB heap+metaspace) which OOMs the 4 GB arm64
# runners alongside the PGO-instrumented binary + load driver. See gh #34.
# The broker is addressed as 127.0.0.1: localhost resolves to ::1 first, where
# the published port may be closed.
KAFKA_CID=$(docker run -d --rm \
    -p 19092:9092 \
    "$KAFKA_IMAGE" \
    redpanda start \
        --mode dev-container \
        --smp 1 \
        --memory 512M \
        --kafka-addr PLAINTEXT://0.0.0.0:9092 \
        --advertise-kafka-addr PLAINTEXT://127.0.0.1:19092)

echo "pgo-workload: Redpanda container: $KAFKA_CID"

# Real protocol readiness via the admin API (rpk), not a bare TCP-open probe.
# `--mode dev-container` bundles --overprovisioned --reserve-memory 0M
# --check=false --unsafe-bypass-fsync and auto-creates topics.
for attempt in $(seq 1 60); do
    if docker exec "$KAFKA_CID" rpk cluster health 2>/dev/null | grep -q "Healthy:.*true"; then
        echo "pgo-workload: Redpanda ready (attempt $attempt)"
        break
    fi
    if [[ $attempt -eq 60 ]]; then
        echo "error: Redpanda did not become ready in 120s" >&2
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

# Every key below is a field of the receiver's Config or scalo's cascade; an
# unknown key is ignored without a warning. Only the DLQ path is expanded.
cat > "$CONFIG_FILE" <<YAML
server:
  bind_address: "127.0.0.1:8080"
  max_body_size: 20971520
  request_timeout_ms: 30000
  max_concurrent_requests: 10000
  tls:
    enabled: false
  auth:
    mode: "none"
    include_common_header: true

grpc:
  enabled: false

otlp:
  enabled: true
  grpc_bind_address: "127.0.0.1:4317"
  http_bind_address: "127.0.0.1:4318"
  mode: "hyperdx"
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
  # Unprivileged ports -- port 514 requires root / CAP_NET_BIND_SERVICE
  # which we don't have in a typical local/CI workload context.
  udp_bind_address: "127.0.0.1:5514"
  tcp_bind_address: "127.0.0.1:5515"
  tls_bind_address: "127.0.0.1:6514"
  max_message_size: 65536
  # Loopback-only load driver; UDP and plain TCP syslog have no handshake.
  accept_unauthenticated: true
  tls:
    enabled: false
  auth:
    mode: "none"

# The Beats wire protocol, which Elastic Agent and Filebeat ship over.
lumberjack:
  enabled: true
  bind_address: "127.0.0.1:5044"
  # Loopback-only load driver, with no client certificates to present.
  accept_unauthenticated: true

fluent:
  enabled: false

gelf:
  enabled: false

# NetFlow / sFlow autosense flow listener.
# Unprivileged ports -- 2055/4739/6343 don't require root.
flow:
  enabled: true
  experimental: false
  bind_address: "127.0.0.1"
  ports:
    - 2055
    - 6343
  output:
    mode: "canonical"
    max_records_per_packet: 1000

kafka:
  brokers:
    - "127.0.0.1:19092"
  client_id: "pgo-workload"

loader:
  transport: "kafka"

destinations:
  default: "kafka"

# Source rules in the shape dfe-engine compiles from Source definitions:
# key_value_set on the identifier the shipper already sends.
routing:
  default_source: "main"
  topic_suffix: "_land"
  source_rules:
    - field: "data_stream.dataset"
      mode: "key_value_set"
      match_value: "cisco_ios.log"
      source: "cisco_ios"
    - field: "data_stream.dataset"
      mode: "key_value_set"
      match_value: "cylance.protect"
      source: "cylance"
    - field: "event.dataset"
      mode: "key_value_set"
      match_value: "panw.panos"
      source: "panw"
  dlq:
    file_path: "${CONFIG_DIR}/dlq"

buffer:
  memory_limit: 0
  pressure_threshold: 0.8

metrics:
  enabled: true
  address: "127.0.0.1:9090"

logger:
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
    if curl -sf -o /dev/null --max-time 1 "http://127.0.0.1:9090/readyz" \
        || curl -sf -o /dev/null --max-time 1 "http://127.0.0.1:8080/readyz"; then
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

if ! PGO_DRIVER_DURATION_SECS="$DURATION" \
    PGO_DRIVER_HTTP_URL="http://127.0.0.1:8080/ingest" \
    PGO_DRIVER_READY_URL="http://127.0.0.1:8080/readyz" \
    PGO_DRIVER_METRICS_URL="http://127.0.0.1:9090/metrics" \
    PGO_DRIVER_LUMBERJACK_ADDR="127.0.0.1:5044" \
    PGO_DRIVER_PROM_RW_URL="http://127.0.0.1:9091/api/v1/write" \
    PGO_DRIVER_HEC_URL="http://127.0.0.1:8088/services/collector/event" \
    PGO_DRIVER_OTLP_HTTP_URL="http://127.0.0.1:4318/v1/logs" \
    PGO_DRIVER_SYSLOG_UDP="127.0.0.1:5514" \
    PGO_DRIVER_SYSLOG_TCP="127.0.0.1:5515" \
    PGO_DRIVER_NETFLOW_ADDR="127.0.0.1:2055" \
    PGO_DRIVER_SFLOW_ADDR="127.0.0.1:6343" \
    "$PGO_DRIVER_PATH"; then
    echo "error: pgo-driver failed" >&2
    tail -50 "$CONFIG_DIR/receiver.log" >&2
    exit 1
fi

echo "pgo-workload: driver complete"

# Each landing topic the source rules route to must hold records, or the
# profile never saw the routing path a deployment takes. The client runs on the
# host network because the broker advertises the host port, which is closed
# inside its own container.
for topic in cisco_ios_land cylance_land panw_land main_land; do
    if [[ -z "$(docker run --rm --network host "$KAFKA_IMAGE" \
        topic consume "$topic" -n 1 -o :end -f '%o\n' -X brokers=127.0.0.1:19092 2>/dev/null)" ]]; then
        echo "error: topic $topic received no records" >&2
        docker run --rm --network host "$KAFKA_IMAGE" topic list -X brokers=127.0.0.1:19092 >&2
        exit 1
    fi
    echo "pgo-workload: topic $topic has records"
done

echo "pgo-workload: done (receiver logs: $CONFIG_DIR/receiver.log)"
