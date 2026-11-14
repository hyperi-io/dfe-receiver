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

### Types

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
| `revert` | Reverts a previous commit | Varies |

### Breaking Changes

For breaking changes that require a major version bump, add `!` after the type
or include `BREAKING CHANGE:` in the footer:

```text
feat!: remove deprecated API endpoints

BREAKING CHANGE: The /v1/users endpoint has been removed. Use /v2/users instead.
```

### Examples

```text
feat(auth): add OAuth2 support for Google login

fix(api): handle null response from upstream service

docs: update installation instructions for Windows

refactor(core)!: restructure module exports

BREAKING CHANGE: Named exports are now used instead of default exports.
```

### Scope

Scope is optional but recommended. Use it to indicate the area of the codebase
affected (e.g., `api`, `auth`, `core`, `cli`, `docs`).

## Semantic Versioning

This project follows [Semantic Versioning 2.0.0](https://semver.org/):

- **MAJOR** (X.0.0): Breaking changes that require users to modify their code
- **MINOR** (0.X.0): New features that are backwards-compatible
- **PATCH** (0.0.X): Bug fixes and minor improvements

Versions are automatically determined by semantic-release based on commit
messages. Do not manually update version numbers.

## Developer Certificate of Origin

This project uses the Developer Certificate of Origin (DCO) to ensure that
contributors have the right to submit their contributions.

By making a contribution to this project, you certify that:

1. The contribution was created in whole or in part by you and you have the
   right to submit it under the license indicated in the file; or

2. The contribution is based upon previous work that, to the best of your
   knowledge, is covered under an appropriate open source license and you
   have the right under that license to submit that work with modifications;
   or

3. The contribution was provided directly to me by some other person who
   certified (1), (2) or (3) and you have not modified it.

4. You understand and agree that this project and the contribution are public
   and that a record of the contribution (including all personal information
   you submit with it, including your sign-off) is maintained indefinitely
   and may be redistributed consistent with this project or the license(s)
   involved.

## How to Sign Off Your Commits

You must sign off each commit to indicate your acceptance of the DCO. Combine
the signoff with your conventional commit message:

```bash
git commit --signoff -m "feat(auth): add two-factor authentication"
```

This produces:

```text
feat(auth): add two-factor authentication

Signed-off-by: Your Name <your.email@example.com>
```

Make sure your Git configuration has your correct name and email:

```bash
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
```

## License for Contributions

All contributions to this project are licensed under the Business Source
License 1.1 (BUSL-1.1), the same license that covers the project.

Each version of the software (including your contributions) will automatically
become available under the Apache License, Version 2.0 on the third
anniversary of its release.

## How to Contribute

1. **Fork the repository** and create your branch from `main`
2. **Make your changes** following the commit message format above
3. **Sign off your commits** with the DCO
4. **Test your changes** to ensure they work as expected
5. **Submit a pull request** with a clear description of what you've done

### Pull Request Checklist

- [ ] Commits follow the conventional commit format
- [ ] All commits are signed off (DCO)
- [ ] Tests pass (if applicable)
- [ ] Documentation is updated (if applicable)

## Run the checks locally before you push

HyperI projects gate every push through CI (lint, format, tests, secret scan,
dependency audit, build). Run the SAME checks locally first so your change lands
green instead of bouncing:

```bash
hyperi-ci check
```

`hyperi-ci` is the HyperI CI CLI (public on PyPI). If you do not have it, install
it once with `uv tool install hyperi-ci` (or `pipx install hyperi-ci`), or run it
ad hoc with `uvx hyperi-ci check`. It runs the project's full local validation --
the same suite CI runs -- and reports what to fix. Run it before every push and before opening a
pull request. It is the single best way to give your change the best chance of
surviving CI.

## CI/CD Workflow

When your pull request is merged to `main`:

1. **semantic-release** analyses commit messages since the last release
2. Determines the next version number based on commit types
3. Generates/updates the CHANGELOG
4. Creates a new GitHub release with release notes
5. Publishes the package (if applicable)

This happens automatically - no manual intervention required.

## Questions

If you have questions about contributing, please open an issue or contact us.

## Context

### What this is

The suite's one external door: eleven wire protocols terminated, every payload
normalised to JSON, stamped, routed to Kafka or straight to dfe-loader over
gRPC. It and dfe-ui are the only components reading untrusted input, so an
advisory here outranks the same one in dfe-loader -- reachability first, per
`dfe-infra/docs/threat-model.md`. It is NOT a transform stage (that is
dfe-loader).

### Where things live

| Path | What is in it |
|---|---|
| `src/main.rs` | CLI, config load, the `--emit-*` generators, the startup order |
| `src/server/` | One directory per handler, plus `traits.rs` (the `ProtocolHandler` boundary), auth, TLS, IP filter. `mod.rs:125` registers them |
| `src/pipeline/`, `routing/`, `validation/` | The shared core path every handler feeds |
| `src/pipeline/acks.rs` | The held answer: admission, hold budget, next-hop deadline |
| `src/buffer/` | `SinkBackend` for answers at enqueue: in-memory, or scalo `TieredSink` with a disk spool |
| `src/sink/` | `kafka/`, `grpc/`, `file/`. Kafka owns its producer so delivery reports are visible |
| `src/config/mod.rs`, `src/deployment.rs` | `Config::validate()`, and the contract the Dockerfile and the released chart are generated from |
| `Dockerfile`, `proto/` | Generated or vendored. Do not hand-edit |
| `tests/` | Targets `smoke`, `integration`, `e2e`. `common/mod.rs` is the container harness |
| `docs/architecture.md` | Why it is built this way, and the invariants |

### Commands that prove a change

```bash
hyperi-ci check                                 # the gate
cargo nextest run                               # unit + integration, incl. the drift guards
cargo nextest run --test e2e --run-ignored all  # adds the 8 Kafka e2e tests (needs Docker)
```

`protoc`, `cmake` and a C toolchain must be present or the build dies inside a
dependency's build script, naming nothing useful. Three ways green lies. The
default run skips eight `#[ignore]` Kafka tests. Container tests skip silently
when Docker is down locally and only panic under `$CI`. And `features: default`
is `otlp` alone, with hyperi-ci adding `--features jemalloc`, so a local build is
not the shipped one.

### What tends to bite

| Don't | Do | Why |
|---|---|---|
| Read `Cargo.toml` for the version | Read `VERSION` | semantic-release writes only `CHANGELOG.md` and `VERSION`. `Cargo.toml` sits at `1.15.10` while `VERSION` is `1.15.36` |
| `cargo test --test integration_kafka` | `--test e2e --run-ignored all` | No such target. This README and `tests/e2e/kafka.rs` both carried it |
| Trust green after Docker was down | Check the skip count, or set `CI=1` | A container test passes without running when Docker is down locally |
| Hand-edit `Dockerfile`, or commit a chart | Fix `src/deployment.rs`, then `--emit-dockerfile Dockerfile` | The Dockerfile is generator output with a test asserting it matches, and the release assembles the chart from the contract, so a hand edit is reverted or never ships |
| Bump scalo and stop | Bump, regenerate, move `release.helm.library` in `.hyperi-ci.yaml` to the same version, commit the diff | The generator is in scalo, so the drift guard fails by design, and a scalo-service release renders only the contract version its scalo release writes |
| Rename a port in `src/deployment.rs` | Keep the name | Deployments name the receiver's ports to wire its listeners, and a rename breaks every one that names the old port |
| Change `Config::validate()` alone | Update dfe-engine's mirror | It hand-copies this validation, nothing compares them, and they have drifted |
| Read 202 as delivered on a listener with acknowledgements off | Compare `receiver_kafka_sends_total` with `receiver_kafka_delivered_total` | That 202 is answered at enqueue |
| Read a 503 as "not written" | Allow for the retry to duplicate the record | The hold can expire while a broker is writing it |
| Set `VAULT_*` on the test OpenBao | Set `BAO_*` | `VAULT_` is ignored, a random root token is minted, everything 403s silently |

### Where this sits

Inbound: **scalo-rs** (crate `scalo`) by `cargo-dep` -- a runtime range plus a dev-dependency range for test support, which move together, and a `generated-file` lockstep edge through the `Dockerfile`, at contract schema version 4. The released chart is assembled on the scalo-service library chart at `release.helm.library`, which moves with the scalo version in `Cargo.toml`.

Outbound: **dfe-infra** by `image-pin` lockstep -- its `versions.yaml` pins
the image and the thin chart released here, and Argo deploys that published
chart. **dfe-engine** by `mirrored-logic` -- it
reimplements `Config::validate()` by hand, so only a human closes that edge.

```bash
python3 ../dfe-infra/scripts/dfe-stack suite --consumer dfe-receiver
python3 ../dfe-infra/scripts/dfe-stack suite --producer dfe-receiver
```
