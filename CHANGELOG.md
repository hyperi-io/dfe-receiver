## [1.14.3](https://github.com/hyperi-io/dfe-receiver/compare/v1.14.2...v1.14.3) (2026-03-24)


### Bug Fixes

* clippy field_reassign_with_default and expect_used in tests ([06bfd0b](https://github.com/hyperi-io/dfe-receiver/commit/06bfd0b76b8b800fb9d34223689c46c9aec59d39))
* inline Renovate config (preset resolution broken) ([a94c02c](https://github.com/hyperi-io/dfe-receiver/commit/a94c02cb1990fad73ab3121964dd32f8cc165bcb))
* prevent double MetricsManager init panic, restructure tests ([e9b02f4](https://github.com/hyperi-io/dfe-receiver/commit/e9b02f4fc8e70e2f776c3067584bbbd9f8dbe6de)), closes [#19](https://github.com/hyperi-io/dfe-receiver/issues/19)
* replace invalid Renovate preset :pinActionsToFullSha with helpers:pinGitHubActionDigestsToSemver ([92b00bc](https://github.com/hyperi-io/dfe-receiver/commit/92b00bc4665d83e7e7af422ce2935afc32e6b198))
* suppress dead_code warnings on shared test helpers ([88fdde0](https://github.com/hyperi-io/dfe-receiver/commit/88fdde059c67eb0890a411487a1ae90e0e0a6fa4))
* update rustls-webpki 0.103.9 → 0.103.10 (GHSA-pwjx-qhcg-rvj4) ([3e92bc6](https://github.com/hyperi-io/dfe-receiver/commit/3e92bc6519d43451ec0f9149616518dad4f7ff46))

## [1.14.2](https://github.com/hyperi-io/dfe-receiver/compare/v1.14.1...v1.14.2) (2026-03-21)


### Bug Fixes

* align VERSION file with latest release tag ([2f831f7](https://github.com/hyperi-io/dfe-receiver/commit/2f831f7335d2f310a60b67776670cf2561563c98))
* trigger release for metrics migration ([fabe429](https://github.com/hyperi-io/dfe-receiver/commit/fabe429117b9381f509c68487505890725959d85))

## [1.14.1](https://github.com/hyperi-io/dfe-receiver/compare/v1.14.0...v1.14.1) (2026-03-19)


### Bug Fixes

* use MemoryGuardConfig::from_env for standard env var overrides ([4a0b302](https://github.com/hyperi-io/dfe-receiver/commit/4a0b30281aca56df7e0bb626f92157d9c07c6d61))

# [1.14.0](https://github.com/hyperi-io/dfe-receiver/compare/v1.13.11...v1.14.0) (2026-03-19)


### Bug Fixes

* clippy explicit_iter_loop in slowloris test ([c592019](https://github.com/hyperi-io/dfe-receiver/commit/c592019))
* code review remediations ([94ca17c](https://github.com/hyperi-io/dfe-receiver/commit/94ca17c))
* emit config_changed security event on pipeline config reload ([7133437](https://github.com/hyperi-io/dfe-receiver/commit/7133437))
* integrate DfeMetrics from rustlib (dual-emit dfe_* alongside receiver_*) ([561e170](https://github.com/hyperi-io/dfe-receiver/commit/561e170))
* internet-facing hardening — slowloris, connection limits, rate limiting, IP filter ([ef7e8df](https://github.com/hyperi-io/dfe-receiver/commit/ef7e8df))
* minor GA readiness items ([44840ec](https://github.com/hyperi-io/dfe-receiver/commit/44840ec))
* remove [patch.crates-io], bump rustlib to >=1.16.3 (published) ([a5992eb](https://github.com/hyperi-io/dfe-receiver/commit/a5992eb))
* replace BufferManager with rustlib MemoryGuard ([69923df](https://github.com/hyperi-io/dfe-receiver/commit/69923df))
* update KEDA PromQL to dfe_scaling_pressure ([631fd25](https://github.com/hyperi-io/dfe-receiver/commit/631fd25))
* use MemoryGuardConfig::from_env for standard env var overrides ([4a0b302](https://github.com/hyperi-io/dfe-receiver/commit/4a0b302))
* wire log spam helpers into identified hot spots ([fbea6f4](https://github.com/hyperi-io/dfe-receiver/commit/fbea6f4))
* wire security event logging into auth, TLS, and config reload ([2526c04](https://github.com/hyperi-io/dfe-receiver/commit/2526c04))


### Features

* add opt-in disk spillover via rustlib TieredSink ([5d798f6](https://github.com/hyperi-io/dfe-receiver/commit/5d798f6))
* add optional Prometheus scaling trigger to KEDA ScaledObject ([d3df594](https://github.com/hyperi-io/dfe-receiver/commit/d3df594))
* add RustlibSinkAdapter for bridging sink traits ([c71cb2f](https://github.com/hyperi-io/dfe-receiver/commit/c71cb2f))
* wire SharedConfig hot-reload to auth state ([9f57c6e](https://github.com/hyperi-io/dfe-receiver/commit/9f57c6e))
