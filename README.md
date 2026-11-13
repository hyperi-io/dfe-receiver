# dfe-receiver

The front door of a DFE deployment: takes events in the protocols agents already speak and hands them on as JSON.

[![Build Status](https://github.com/hyperi-io/dfe-receiver/actions/workflows/ci.yml/badge.svg)](https://github.com/hyperi-io/dfe-receiver/actions)
[![License](https://img.shields.io/badge/license-BUSL--1.1-blue)](https://github.com/hyperi-io/dfe-receiver/blob/main/LICENSE)

## What it does

- One service for HTTP(S) JSON, Vector's gRPC sink, OTLP, Beats, Splunk HEC, syslog, Fluent Forward, GELF, Prometheus Remote Write and webhooks, so an agent keeps the protocol it already speaks. Every handler normalises to JSON and hands the record to Kafka or straight to dfe-loader over gRPC.
- Ordered routing rules that set each record's source and org from fields it already carries, such as `data_stream.dataset`, and pick its Kafka topic from the source.
- A sender answered only once every destination has confirmed its records, on by default for each listener that can hold its answer. `POST /ingest` answers 202 when the batch is accepted, 503 with `Retry-After` when it cannot take it, and 413 over the body limit, 10,485,760 bytes by default.
- Each record's org held to what its sender may use. A credential or listener bound in `ingest_bindings` lands only in its own orgs, and an unbound sender only in the deployment's default org.
- Header, bearer token and mTLS authentication.
- Built on [scalo](https://github.com/hyperi-io/scalo-rs), so config, logs, metrics, health probes and memory backpressure work the way they do in every DFE Rust service.

| Protocol | Port | Source |
|---|---|---|
| HTTP(S) JSON | configurable | Vector, custom agents |
| gRPC Vector sink | configurable | Vector |
| Webhook (`POST /webhook/{caller}`) | shares HTTP, or 8090 | Product alert rules and notification hooks (runZero, ...) |
| OTLP gRPC | 4317 | OpenTelemetry collectors |
| OTLP HTTP | 4318 | OpenTelemetry collectors |
| Lumberjack/Beats | 5044 | Filebeat, Logstash Beats output |
| Splunk HEC | 8088 | Splunk forwarders, Fluentd HEC output |
| Syslog UDP/TCP/TLS | 514 / 6514 | syslogd, rsyslog, syslog-ng |
| Fluent Forward | 24224 | Fluentd, Fluent Bit |
| GELF TCP | 12201 | Graylog GELF output, Fluent Bit |
| Prometheus Remote Write | 9091 | Prometheus, VictoriaMetrics |
| Flow (NetFlow + sFlow), experimental | 2055 / 4739 / 6343 UDP | NetFlow v5/v9, IPFIX, sFlow v5 exporters (v7 not supported) |

HTTP always runs, because the probes ride its listener, and the other listeners are opt-in. A listener that answers at enqueue (acknowledgements off, or syslog, GELF and flow) buffers in memory behind a circuit breaker, and disk spillover is opt-in.

## Quick start

The build needs `protoc`, `cmake` and a C toolchain on `PATH`: the gRPC, OTLP and Prometheus Remote Write protocols compile vendored `.proto` files, and `aws-lc-sys` and `rdkafka-sys` build native code.

```bash
sudo apt-get install -y protobuf-compiler cmake build-essential   # Debian or Ubuntu
brew install protobuf cmake                                        # macOS
cargo build --release
./target/release/dfe-receiver --config config.yaml
```

The smallest config that moves data names the bind address, an auth mode, the brokers and the default source:

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

Every setting is in [config.example.yaml](config.example.yaml), and `DFE_RECEIVER_SERVER__BIND_ADDRESS` is the shape of an environment override: the `DFE_RECEIVER_` prefix, then the key path with double underscores. `server.auth.mode` is `none`, `header`, `bearer` or `mtls`. The routing rules, the webhook intake, the metrics and the hot-path design are in [docs/internals.md](docs/internals.md).

## Documentation

- [docs/internals.md](docs/internals.md): how it works, configuration, the webhook intake, metrics and the security model.
- [docs/hardening.md](docs/hardening.md): running it where it is reachable from outside.
- [docs/sidecar-transports.md](docs/sidecar-transports.md) and [docs/webhook-setup.md](docs/webhook-setup.md): the transport options and the webhook callers.
- [docs/metrics.md](docs/metrics.md) and [docs/performance.md](docs/performance.md): what it reports and what it costs.
- [docs/architecture.md](docs/architecture.md): where it sits in the suite and why it is shaped this way.

The suite's published docs are at <https://dfe.docs.hyperi.io>, and this repository's pages at <https://dfe.docs.hyperi.io/dfe-receiver/>.

## Help and contributing

How to work in this repo, the commands that prove a change and what tends to bite are in [CONTRIBUTING.md](CONTRIBUTING.md). A question that is not a bug goes in a new issue.

Open bugs: [this repo](https://github.com/hyperi-io/dfe-receiver/issues?q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor) | [whole DFE suite](https://github.com/search?type=issues&q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor+repo%3Ahyperi-io%2Fdfe-infra+repo%3Ahyperi-io%2Fdfe-docker+repo%3Ahyperi-io%2Fdfe-deploy+repo%3Ahyperi-io%2Fdfe-engine+repo%3Ahyperi-io%2Fdfe-loader+repo%3Ahyperi-io%2Fdfe-receiver+repo%3Ahyperi-io%2Fdfe-fetcher+repo%3Ahyperi-io%2Fdfe-archiver+repo%3Ahyperi-io%2Fdfe-transform-vrl+repo%3Ahyperi-io%2Fdfe-transform-vector+repo%3Ahyperi-io%2Fdfe-transform-elastic+repo%3Ahyperi-io%2Fdfe-ui+repo%3Ahyperi-io%2Fdfe-hyperdx+repo%3Ahyperi-io%2Fscalo-rs+repo%3Ahyperi-io%2Fscalo-py+repo%3Ahyperi-io%2Flogreducer+repo%3Ahyperi-io%2Fdfe-schemas+repo%3Ahyperi-io%2Fvector-vrl+repo%3Ahyperi-io%2Fculvert+repo%3Ahyperi-io%2Fdfe-clickhouse-rs+repo%3Ahyperi-io%2Ffactbook-rs)

## License

BUSL-1.1, see [LICENSE](LICENSE).
