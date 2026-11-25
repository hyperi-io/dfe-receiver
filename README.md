# dfe-receiver

The front door of a DFE deployment: takes events in the protocols agents already speak and hands them on as JSON.

[![Build Status](https://github.com/hyperi-io/dfe-receiver/actions/workflows/ci.yml/badge.svg)](https://github.com/hyperi-io/dfe-receiver/actions)
[![License](https://img.shields.io/badge/license-BUSL--1.1-blue)](https://github.com/hyperi-io/dfe-receiver/blob/main/LICENSE)

## What it does

- One service for HTTP(S) JSON, Vector's gRPC sink, OTLP, Beats, Splunk HEC, syslog, Fluent Forward, GELF, Prometheus Remote Write and webhooks, so an agent keeps the protocol it speaks. Every handler normalises to JSON and hands the record to Kafka or straight to dfe-loader over gRPC.
- Ordered routing rules that set each record's source and org from fields it has, such as `data_stream.dataset`, and pick its Kafka topic from the source.
- A sender answered once every destination has confirmed its records, on by default for each listener that can hold its answer. `POST /ingest` answers 202 when the batch is accepted, 503 with `Retry-After` when it cannot take it, and 413 over the body limit, 10,485,760 bytes by default.
- Each record's org held to what its sender may use. A credential or listener bound in `ingest_bindings` lands in its bound orgs alone, and an unbound sender in the deployment's default org.
- Header, bearer token and mTLS authentication.
- Built on [scalo](https://github.com/hyperi-io/scalo-rs), so config, logs, metrics, health probes and memory backpressure work the way they do in every DFE Rust service.

| Protocol | Port | Source |
|---|---|---|
| HTTP(S) JSON | 8080 | Vector, [NXLog](docs/nxlog.md), custom agents |
| gRPC Vector sink | 6000 | Vector |
| Webhook (`POST /webhook/{caller}`) | shares HTTP, or 8090 | Product alert rules and notification hooks (runZero, ...) |
| OTLP gRPC | 4317 | OpenTelemetry collectors |
| OTLP HTTP | 4318 | OpenTelemetry collectors |
| Lumberjack/Beats | 5044 | Filebeat, Logstash Beats output, [Zeek](docs/zeek.md) and [Suricata](docs/suricata.md) through Filebeat |
| Splunk HEC | 8088 | Splunk forwarders, Fluentd HEC output |
| Syslog UDP/TCP/TLS | 514 / 6514 | syslogd, rsyslog, syslog-ng, NXLog |
| Fluent Forward | 24224 | Fluentd, Fluent Bit, Zeek and Suricata logs through Fluent Bit |
| GELF TCP | 12201 | Graylog GELF output, Fluent Bit, NXLog |
| Prometheus Remote Write | 9091 | Prometheus, VictoriaMetrics |
| Flow (NetFlow + sFlow) | 2055 / 4739 / 6343 UDP | NetFlow v5/v9, IPFIX, sFlow v5 exporters (v7 not supported) |

HTTP always runs, and the other listeners are opt-in. A listener that answers at enqueue (acknowledgements off, or syslog, GELF and flow) buffers in memory behind a circuit breaker, and disk spillover is opt-in.

## Quick start

The build needs `protoc`, a C toolchain, `pkg-config` and an installed librdkafka. The gRPC, OTLP and Prometheus Remote Write protocols compile the `.proto` files copied into `proto/`, `aws-lc-sys` compiles C, and `rdkafka-sys` links librdkafka through pkg-config. It rejects a librdkafka older than the version after the `+` in its `Cargo.lock` entry, and a Debian or Ubuntu package can be older than that, so there [build librdkafka from source](https://github.com/confluentinc/librdkafka#build-from-source) first.

```bash
sudo apt-get install -y protobuf-compiler build-essential pkg-config   # Debian or Ubuntu
brew install protobuf pkg-config librdkafka                              # macOS
cargo build --release
./target/release/dfe-receiver --config config.yaml
```

The brokers are the one key with no default, so this is the smallest config that moves data. The other keys are written out at their defaults:

```yaml
server:
  bind_address: "0.0.0.0:8080"
  auth:
    mode: none

kafka:
  brokers:
    - "localhost:9092"

routing:
  default_source: "main"
  topic_suffix: "_land"
```

## Configuration

[docs/config-schema.yaml](docs/config-schema.yaml) lists every setting with its default, generated from the code, and [config.example.yaml](config.example.yaml) is an annotated starting point. Started with `--config`, as the image is, the receiver reads a fixed set of flat environment variables over the file, such as `DFE_RECEIVER_BIND_ADDRESS` and `DFE_RECEIVER_KAFKA_BROKERS`. Started without it, scalo's cascade reads any key as its path with double underscores as well, `DFE_RECEIVER_SERVER__BIND_ADDRESS`. `server.auth.mode` is `none`, `header`, `bearer`, `mtls` or `both`.

## Documentation

- [docs/architecture.md](docs/architecture.md): where it sits in the suite and why it is built this way.
- [docs/delivery.md](docs/delivery.md): when a sender is answered, and what the receiver buffers.
- [docs/internals.md](docs/internals.md): the shared path -- what it rejects, routing, what a sender is told, auth, Kafka and deployment.
- [docs/protocols.md](docs/protocols.md): what each protocol parses, raw capture, and flow.
- [docs/hardening.md](docs/hardening.md): running it where it is reachable from outside.
- [docs/sidecar-transports.md](docs/sidecar-transports.md) and [docs/webhook-setup.md](docs/webhook-setup.md): the transport options and the webhook callers.
- [docs/metrics.md](docs/metrics.md) and [docs/performance.md](docs/performance.md): what it reports and what it costs.

The suite's published docs are at <https://dfe.docs.hyperi.io>, and this repository's pages at <https://dfe.docs.hyperi.io/dfe-receiver/>.

## Help and contributing

How to work in this repo, the commands that test a change and what tends to bite are in [CONTRIBUTING.md](CONTRIBUTING.md). A question that is not a bug goes in an issue.

Open bugs: [this repo](https://github.com/hyperi-io/dfe-receiver/issues?q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor) | [whole DFE suite](https://github.com/search?type=issues&q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor+repo%3Ahyperi-io%2Fdfe-infra+repo%3Ahyperi-io%2Fdfe-docker+repo%3Ahyperi-io%2Fdfe-deploy+repo%3Ahyperi-io%2Fdfe-engine+repo%3Ahyperi-io%2Fdfe-loader+repo%3Ahyperi-io%2Fdfe-receiver+repo%3Ahyperi-io%2Fdfe-fetcher+repo%3Ahyperi-io%2Fdfe-archiver+repo%3Ahyperi-io%2Fdfe-transform-vrl+repo%3Ahyperi-io%2Fdfe-transform-vector+repo%3Ahyperi-io%2Fdfe-transform-elastic+repo%3Ahyperi-io%2Fdfe-ui+repo%3Ahyperi-io%2Fdfe-hyperdx+repo%3Ahyperi-io%2Fscalo-rs+repo%3Ahyperi-io%2Fscalo-py+repo%3Ahyperi-io%2Flogreducer+repo%3Ahyperi-io%2Fdfe-schemas+repo%3Ahyperi-io%2Fvector-vrl+repo%3Ahyperi-io%2Fculvert+repo%3Ahyperi-io%2Fdfe-clickhouse-rs+repo%3Ahyperi-io%2Ffactbook-rs)

## License

BUSL-1.1, see [LICENSE](LICENSE).
