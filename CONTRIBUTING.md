# Contributing

We welcome contributions to this project. By contributing, you agree to the
terms outlined below.

## Commit Message Format

HyperI projects use [Conventional Commits](https://www.conventionalcommits.org/)
and [semantic-release](https://semantic-release.gitbook.io/) for automated
versioning and changelog generation. All commits must follow this format:

```text
<type>(<scope>): <subject>

[optional body]

[optional footer(s)]
```

Each type sets the version bump:

| Type | Description | Version Bump |
|------|-------------|--------------|
| `feat` | A new feature | Minor (0.X.0) |
| `fix` | A bug fix | Patch (0.0.X) |
| `docs` | Documentation only | None |
| `style` | Code style (formatting, semicolons, etc.) | None |
| `refactor` | Code change that neither fixes a bug nor adds a feature | None |
| `perf` | Performance improvement | Patch (0.0.X) |
| `test` | Adding or correcting tests | None |
| `build` | Changes to build system or dependencies | None |
| `ci` | Changes to CI configuration | None |
| `chore` | Other changes that don't modify src or test files | None |
| `revert` | Reverts a previous commit | Patch (0.0.X) |

For breaking changes that require a major version bump, add `!` after the type
or include `BREAKING CHANGE:` in the footer:

```text
feat!: remove deprecated API endpoints

BREAKING CHANGE: The /v1/users endpoint has been removed. Use /v2/users instead.
```

Examples:

```text
feat(auth): add OAuth2 support for Google login

fix(api): handle null response from upstream service

docs: update installation instructions for Windows

refactor(core)!: restructure module exports

BREAKING CHANGE: Named exports are now used instead of default exports.
```

Scope is optional but recommended. Use it to indicate the area of the codebase
affected (e.g., `api`, `auth`, `core`, `cli`, `docs`).

This project follows [Semantic Versioning 2.0.0](https://semver.org/):

- MAJOR (X.0.0): breaking changes that require users to modify their code
- MINOR (0.X.0): new features that are backwards-compatible
- PATCH (0.0.X): bug fixes and minor improvements

Versions are automatically determined by semantic-release based on commit
messages. Do not manually update version numbers.

## License for Contributions

All contributions to this project are licensed under the Business Source
License 1.1 (BUSL-1.1), the same license that covers the project.

Each version of the software (including your contributions) will automatically
become available under the Apache License, Version 2.0 on the third
anniversary of its release.

## How to Contribute

1. Fork the repository and create your branch from `main`
2. Make your changes, following the commit message format above
3. Test your changes
4. Open a pull request saying what it changes and why

Questions about contributing go in an issue.

Before opening the pull request:

- [ ] Commits follow the conventional commit format
- [ ] Tests pass (if applicable)
- [ ] Documentation is updated (if applicable)

HyperI projects gate every push through CI (lint, format, tests, secret scan,
dependency audit, build). Run the SAME checks locally first so your change lands
passing instead of bouncing:

```bash
hyperi-ci check
```

`hyperi-ci` is the HyperI CI CLI (public on PyPI). If you do not have it, install
it once with `uv tool install hyperi-ci` (or `pipx install hyperi-ci`), or run it
ad hoc with `uvx hyperi-ci check`. It runs the project's full local validation --
the same suite CI runs -- and reports what to fix. Run it before every push and before opening a
pull request. It is the single best way to give your change the best chance of
surviving CI.

A merge to `main` runs CI and releases nothing by itself. A release is cut when the squash-merge commit ends with a `Release: true` trailer, or when a maintainer dispatches the CI workflow with `from-head: true`. semantic-release then reads the commit messages since the last release and picks the next version, the release commit updates `CHANGELOG.md` and `VERSION`, and hyperi-ci builds and publishes the release.

## Context

### What this is

The suite's ingest edge: every wire protocol in the README's table terminated, every payload normalised to JSON, stamped, and routed to Kafka or straight to dfe-loader over gRPC. With dfe-ui, dfe-hyperdx and, where it is deployed, culvert, it is one of the few components reading untrusted input, so an advisory here outranks the same one in dfe-loader -- reachability first, per `dfe-infra/docs/threat-model.md`. It parses the transport framing and stops there. Parsing a message body is the dfe-transform-* apps' job, and fitting a record to its table is dfe-loader's.

### Where things live

| Path | What is in it |
|---|---|
| `src/main.rs` | CLI, config load, the `--emit-dockerfile` flag, the startup order |
| `src/server/` | One directory per handler, plus `traits.rs` (the `ProtocolHandler` boundary), auth, TLS, IP filter. `Server::build_handlers` in `mod.rs` registers them |
| `src/pipeline/`, `routing/`, `validation/` | The shared core path every handler feeds |
| `src/pipeline/acks.rs` | The held answer: admission, hold budget, next-hop deadline |
| `src/buffer/` | `SinkBackend` for answers at enqueue: in-memory, or scalo `TieredSink` with a disk spool |
| `src/sink/` | `kafka/`, `grpc/`, `file/`. Kafka owns its producer so delivery reports are visible |
| `src/config/mod.rs`, `src/deployment.rs` | `Config::validate()`, and the contract the Dockerfile and the released chart are generated from |
| `Dockerfile`, `proto/` | Generated, or copied in from upstream. Do not hand-edit |
| `tests/` | Targets `smoke`, `integration`, `e2e`, `orchestrator_lifecycle`. `common/mod.rs` is the container harness |
| `docs/architecture.md` | What the receiver is and why it's built this way |

### Commands that prove a change

```bash
hyperi-ci check                                 # the gate
cargo nextest run                               # unit + integration, incl. the drift guards
cargo nextest run --test e2e --run-ignored all  # adds the ignored Kafka e2e tests (needs Docker)
```

The build needs `protoc`, a C toolchain, `pkg-config` and librdkafka, as the README's quick start says. Three ways a passing run lies. The
default run skips the `#[ignore]` Kafka tests. Container tests skip silently
when Docker is down locally and panic under `$CI` alone. And `features: default`
is `otlp` alone, with hyperi-ci adding `--features jemalloc`, so a local build is
not the shipped one.

### What tends to bite

| Don't | Do | Why |
|---|---|---|
| Read `Cargo.toml` for the version | Read `VERSION` | semantic-release writes `CHANGELOG.md` and `VERSION` and no other file, so the version in `Cargo.toml` lags |
| `cargo test --test integration_kafka` | `--test e2e --run-ignored all` | No such target |
| Trust a pass after Docker was down | Check the skip count, or set `CI=1` | A container test passes without running when Docker is down locally |
| Hand-edit `Dockerfile`, or commit a chart | Fix `src/deployment.rs`, then `--emit-dockerfile Dockerfile` | The Dockerfile is generator output with a test asserting it matches, and the release assembles the chart from the contract, so a hand edit is reverted or never ships |
| Bump scalo and stop | Bump, regenerate, move `release.helm.library` in `.hyperi-ci.yaml` to the same version, commit the diff | The generator is in scalo, so the drift guard fails by design, and a scalo-service release renders the contract version its scalo release writes and no other |
| Rename a port in `src/deployment.rs` | Keep the name | Deployments name the receiver's ports to wire its listeners, and a rename breaks every one that names the old port |
| Change the credential id rule or the bearer listeners alone | Change dfe-engine's copies with them | The engine rejects an ingest credential by a separate copy of both (`src/dfe_engine/orgs/ingest_credentials.py` and `src/dfe_engine/api/v1/ingest_credentials.py` in dfe-engine), and nothing compares the two |
| Read 202 as delivered on a listener with acknowledgements off | Compare `receiver_kafka_sends_total` with `receiver_kafka_delivered_total` | That 202 is answered at enqueue |
| Read a 503 as "not written" | Allow for the retry to duplicate the record | The hold can expire with a broker part way through writing it |
| Set `VAULT_*` on the test OpenBao | Set `BAO_*` | `VAULT_` is ignored, a random root token is minted, everything 403s silently |
| Add a protocol by touching the core path | Add a directory under `src/server/` implementing `ProtocolHandler`, a config section, a block in `build_handlers` and a port in `src/deployment.rs` | A handler owns its socket, framing and decode, and hands JSON to the one shared pipeline. `listeners()` returns one `BoundAddr` per socket, which `/readyz` waits on |
| Read a passing test as proof a YAML file is accepted | Load the file through `load_config` | `Config::validate()` runs in `load_config` (`src/main.rs`) on every start, and a test that builds a `Config` struct directly never reaches it |

### Where this sits

Inbound: **scalo-rs** (crate `scalo`) by `cargo-dep` -- a runtime range plus a dev-dependency range for test support, which move together, and a `generated-file` lockstep edge through the `Dockerfile`, at contract schema version 4. The released chart is assembled on the scalo-service library chart at `release.helm.library`, which moves with the scalo version in `Cargo.toml`.

Outbound: **dfe-infra** by `image-pin` lockstep -- its `versions.yaml` pins
the image and the thin chart released here, and Argo deploys that published
chart. **dfe-engine** by `mirrored-logic` -- it keeps a copy of the credential id rule and the bearer listeners, so a human closes that edge and no check does.

```bash
python3 ../dfe-infra/scripts/dfe-stack suite --consumer dfe-receiver
python3 ../dfe-infra/scripts/dfe-stack suite --producer dfe-receiver
```
