# Testing

How dfe-receiver's tests split into tiers, and the containers a run shares.

| Level | Runs | Command |
|---|---|---|
| core | locally, every push and PR | `cargo nextest run` |
| full | nightly (`schedule` in `.github/workflows/ci.yml`) and before every release (`test.full.required_for_release: true`) | `hyperi-ci check --tier full`, or `cargo nextest run --run-ignored all` |

Core is everything nextest runs by default: the unit tests in `src/`, the smoke and orchestrator tests, and `tests/integration/`, the Kafka, OpenBao, TLS and peer-tool tests included. Full adds the `#[ignore]` tests in `tests/e2e/kafka.rs`, which drive a whole HTTP to Kafka pipeline against the broker `kafka_backend` picks.

A test that needs Docker skips without a daemon on a developer's machine and fails with `CI` set, so CI never passes a test it did not run.

`TEST_MODE`, from the environment or the repo's `.env`, picks the Kafka the tests use (`tests/common/mod.rs`). `live`, the default, takes the cluster `KAFKA_BROKERS` names, else the run's shared broker below. `testcontainers` always takes the shared broker. `docker` takes the broker `docker-compose.test.yaml` runs on `localhost:19092`, and a named cluster that is unreachable or refuses the credentials fails the test rather than skipping it:

```bash
docker compose -f docker-compose.test.yaml up -d
env TEST_MODE=docker cargo nextest run --test integration
kcat -b localhost:19092 -t <test topic> -C -o beginning   # the topic a test names
docker compose -f docker-compose.test.yaml down -v
```

## Services, one instance each per run

| Service | Image | Port |
|---|---|---|
| Kafka | `apache/kafka` | 9092 PLAINTEXT |
| OpenBao | `openbao/openbao` | 8200, dev server |

The Kafka pin is scalo's `transport::kafka::test_support::KAFKA_IMAGE`. The OpenBao pin is `OPENBAO_IMAGE` in `tests/common/mod.rs`.

Both come from scalo's `test_support::shared_service`, through `shared_kafka` and `shared_openbao` in `tests/common/mod.rs`:

- The first test process to ask starts the container, named `dfe-receiver-test-integration-<service>-r<run>`, and every other process joins it by that name.
- The starter also runs a watcher that removes the container once the nextest process exits, on a pass, a failure or Ctrl-C. Each start and removal is logged to `shared-services-<run>.log` under the target's `tmp/`.
- A start first sweeps this suite's containers whose owner process has gone.
- Docker is whatever the environment names: `DOCKER_HOST`, else the current docker context. Ports bind to loopback on the daemon host, and an `ssh://` daemon gets one tunnel per port. In CI it is the runner's daemon.

Tests on the shared broker keep to their own topics (`test_topic`), and tests on OpenBao to their own secret paths. Three kinds of test hold a container of their own, removed when the test drops it:

- the two Kafka sink tests that pause their broker (`start_kafka_container`)
- the Kafka TLS test, on scalo's `TlsBroker`: one KRaft broker with SSL, SASL_SSL and mTLS listeners and a private CA made for the run
- the peer-tool tests in `tests/common/peers.rs` (Fluent Bit, the OpenTelemetry Collector, Filebeat, Logstash, Vector), each pinned by digest there
