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

