## [1.15.6](https://github.com/hyperi-io/dfe-receiver/compare/v1.15.5...v1.15.6) (2026-04-19)


### Bug Fixes

* Tier 2 canary on hyperi-ci v1.10.1 (extended workload grace) ([8ecd308](https://github.com/hyperi-io/dfe-receiver/commit/8ecd30831873a7f00917ec7eb181e58c07b7ca1c))

## [1.15.5](https://github.com/hyperi-io/dfe-receiver/compare/v1.15.4...v1.15.5) (2026-04-19)


### Bug Fixes

* Tier 2 canary on hyperi-ci v1.10.0 (universal tool install) ([37b60cc](https://github.com/hyperi-io/dfe-receiver/commit/37b60cc89ed5f99bb69e28487a41d0cafb51606e))

## [1.15.4](https://github.com/hyperi-io/dfe-receiver/compare/v1.15.3...v1.15.4) (2026-04-19)


### Bug Fixes

* **pgo:** build pgo-driver on-demand during workload orchestration ([f9108d2](https://github.com/hyperi-io/dfe-receiver/commit/f9108d25a9d79ffa6f612196debe113825e2c67f))

## [1.15.3](https://github.com/hyperi-io/dfe-receiver/compare/v1.15.2...v1.15.3) (2026-04-18)


### Bug Fixes

* Tier 2 canary on hyperi-ci v1.9.6 workload-arg contract ([a3bd00b](https://github.com/hyperi-io/dfe-receiver/commit/a3bd00bf416e9fca75af6c1506f3a15996677519))

## [1.15.2](https://github.com/hyperi-io/dfe-receiver/compare/v1.15.1...v1.15.2) (2026-04-18)


### Bug Fixes

* Tier 2 canary retrigger on hyperi-ci v1.9.5 ([b25762f](https://github.com/hyperi-io/dfe-receiver/commit/b25762fa4e7ceec8ca5ada2360703106e0ccf3af))

## [1.15.1](https://github.com/hyperi-io/dfe-receiver/compare/v1.15.0...v1.15.1) (2026-04-18)


### Bug Fixes

* retrigger Tier 2 canary on hyperi-ci v1.9.4 channel resolver ([9e36479](https://github.com/hyperi-io/dfe-receiver/commit/9e36479dd56b34992e7291007aa304ba45cfddf3))

# [1.15.0](https://github.com/hyperi-io/dfe-receiver/compare/v1.14.10...v1.15.0) (2026-04-18)


### Bug Fixes

* **ci:** unpin hyperi-ci workflow from v1.5.0 digest to [@main](https://github.com/main) ([068a92a](https://github.com/hyperi-io/dfe-receiver/commit/068a92af900fd2f0525ee2170d308c911dbb1f9e)), closes [#27](https://github.com/hyperi-io/dfe-receiver/issues/27)
* clippy lints introduced in Rust 1.95 ([74515db](https://github.com/hyperi-io/dfe-receiver/commit/74515dbf107c873801d648c6624404681e70b772))
* opt in to hyperi-ci Tier 2 PGO + BOLT on release channel ([89b5941](https://github.com/hyperi-io/dfe-receiver/commit/89b5941cd896cbf561cacf5a145e0b71035d0803))


### Features

* add PGO workload driver + performance docs ([a31d9b2](https://github.com/hyperi-io/dfe-receiver/commit/a31d9b2aed8bc954318e88041b1de37ee77c8773))

## [1.14.10](https://github.com/hyperi-io/dfe-receiver/compare/v1.14.9...v1.14.10) (2026-04-16)


### Bug Fixes

* bump hyperi-rustlib to >=2.5.4, handle FilteredDlq variant ([e11f834](https://github.com/hyperi-io/dfe-receiver/commit/e11f834d2d7b7ac6bcddc0be3a426aee78270cda))
* security hardening and dependency updates ([763ab97](https://github.com/hyperi-io/dfe-receiver/commit/763ab9706e4a3b84e7a3528475dc4367bcb55c9d))

## [1.14.9](https://github.com/hyperi-io/dfe-receiver/compare/v1.14.8...v1.14.9) (2026-04-03)


### Bug Fixes

* add gitignore entry to trigger CI for semantic-release ([6cb0b39](https://github.com/hyperi-io/dfe-receiver/commit/6cb0b39eb40e49b82723e25d156e99dd2f85ba38))
* add process_batch() for multi-message handler batching ([0ee57df](https://github.com/hyperi-io/dfe-receiver/commit/0ee57df098f04f1751eb62490025fa81d636abe6))
* force CI for semantic-release — process_batch integration ([333d515](https://github.com/hyperi-io/dfe-receiver/commit/333d515c696f95c825c0ad0dc13a34276cf5c244))
* re-trigger semantic-release for process_batch changes ([08c0806](https://github.com/hyperi-io/dfe-receiver/commit/08c0806f38ad85e1cc445f49a1bc6fc8c68fdc9d))

## [1.14.8](https://github.com/hyperi-io/dfe-receiver/compare/v1.14.7...v1.14.8) (2026-04-02)


### Bug Fixes

* remove duplicate schema_version and oci_labels fields ([10b5541](https://github.com/hyperi-io/dfe-receiver/commit/10b55410099e1fd3b601ac84edcb198766fa297c))
* remove orphan ci submodule reference — breaks checkout on CI ([3dddaf3](https://github.com/hyperi-io/dfe-receiver/commit/3dddaf3fa0f13d412d4c0bfa32fee1f676d49f78))
* remove tracked target symlink — breaks CI runners ([bc46a99](https://github.com/hyperi-io/dfe-receiver/commit/bc46a99fadd62481b6e48f2dbe3ad0d60899ab82))
* retrigger CI after runner reset ([3fb275d](https://github.com/hyperi-io/dfe-receiver/commit/3fb275ddcf0cdbf04ab01079b020eaaca476d476))

## [1.14.7](https://github.com/hyperi-io/dfe-receiver/compare/v1.14.6...v1.14.7) (2026-04-02)


### Bug Fixes

* add debug and trace logging for request handling, routing, and sinks ([9dc8d2a](https://github.com/hyperi-io/dfe-receiver/commit/9dc8d2a06cfcb36b1fa5ff41ea7cadab3c7e3c14))
* add receiver batching design spec for Phase 2 ([5983766](https://github.com/hyperi-io/dfe-receiver/commit/598376647bdb120a53627c5fad71777fef863297))
* add worker feature for future parallel batch validation ([0e5c607](https://github.com/hyperi-io/dfe-receiver/commit/0e5c6076b7261a715ff411dd8d680c7fcca09d27))
* bump hyperi-rustlib to >=2.4.3 and add DeploymentContract fields ([9f84f29](https://github.com/hyperi-io/dfe-receiver/commit/9f84f29754ed20a99d9bf0391050e97c7a415773))
* Mismatched type error ([1d1ad8e](https://github.com/hyperi-io/dfe-receiver/commit/1d1ad8e6d2a1701b072b552582d09929d27ecefa))
* update DfeMetrics::register() to pass &MetricsManager for manifest ([b1def36](https://github.com/hyperi-io/dfe-receiver/commit/b1def36341915b098374847d002fd5198c8bde40))
* update to rustlib v2.x ServiceRuntime + releaserc breaking rule ([6db440c](https://github.com/hyperi-io/dfe-receiver/commit/6db440cf73c3415c9ed8c2d9346aacedcf57c4ef))
* use ServiceRuntime metrics manager to avoid double recorder panic ([b50755d](https://github.com/hyperi-io/dfe-receiver/commit/b50755d3ef7a26d20d174000356e995b6036d70e))

## [1.14.6](https://github.com/hyperi-io/dfe-receiver/compare/v1.14.5...v1.14.6) (2026-03-29)


### Bug Fixes

* add request duration histogram, active connections gauge, hot path optimisations ([04eae99](https://github.com/hyperi-io/dfe-receiver/commit/04eae9993b9280900aeb50fb195ad3a8181e0ba6))

## [1.14.5](https://github.com/hyperi-io/dfe-receiver/compare/v1.14.4...v1.14.5) (2026-03-27)


### Bug Fixes

* update hyperi-ai submodule to latest standards ([9e3570b](https://github.com/hyperi-io/dfe-receiver/commit/9e3570baa7ef41cbb7f7d43c377eb30dea324a5f))

# [1.14.0-dev.8](https://github.com/hyperi-io/dfe-receiver/compare/v1.14.0-dev.7...v1.14.0-dev.8) (2026-03-25)


### Bug Fixes

* add version check on startup, document crates.io-only rustlib rule ([7021222](https://github.com/hyperi-io/dfe-receiver/commit/70212224338907a7f3701bb8cc5f92a9ad54783f))

# [1.14.0-dev.7](https://github.com/hyperi-io/dfe-receiver/compare/v1.14.0-dev.6...v1.14.0-dev.7) (2026-03-24)


### Bug Fixes

* clippy field_reassign_with_default and expect_used in tests ([06bfd0b](https://github.com/hyperi-io/dfe-receiver/commit/06bfd0b76b8b800fb9d34223689c46c9aec59d39))
* prevent double MetricsManager init panic, restructure tests ([e9b02f4](https://github.com/hyperi-io/dfe-receiver/commit/e9b02f4fc8e70e2f776c3067584bbbd9f8dbe6de)), closes [#19](https://github.com/hyperi-io/dfe-receiver/issues/19)
* suppress dead_code warnings on shared test helpers ([88fdde0](https://github.com/hyperi-io/dfe-receiver/commit/88fdde059c67eb0890a411487a1ae90e0e0a6fa4))
* update rustls-webpki 0.103.9 → 0.103.10 (GHSA-pwjx-qhcg-rvj4) ([3e92bc6](https://github.com/hyperi-io/dfe-receiver/commit/3e92bc6519d43451ec0f9149616518dad4f7ff46))

# [1.14.0-dev.6](https://github.com/hyperi-io/dfe-receiver/compare/v1.14.0-dev.5...v1.14.0-dev.6) (2026-03-23)


### Bug Fixes

* replace invalid Renovate preset :pinActionsToFullSha with helpers:pinGitHubActionDigestsToSemver ([92b00bc](https://github.com/hyperi-io/dfe-receiver/commit/92b00bc4665d83e7e7af422ce2935afc32e6b198))

# [1.14.0-dev.5](https://github.com/hyperi-io/dfe-receiver/compare/v1.14.0-dev.4...v1.14.0-dev.5) (2026-03-22)


### Bug Fixes

* inline Renovate config (preset resolution broken) ([a94c02c](https://github.com/hyperi-io/dfe-receiver/commit/a94c02cb1990fad73ab3121964dd32f8cc165bcb))

# [1.14.0-dev.4](https://github.com/hyperi-io/dfe-receiver/compare/v1.14.0-dev.3...v1.14.0-dev.4) (2026-03-21)


### Bug Fixes

* align VERSION file with latest release tag ([2f831f7](https://github.com/hyperi-io/dfe-receiver/commit/2f831f7335d2f310a60b67776670cf2561563c98))
* trigger release for metrics migration ([fabe429](https://github.com/hyperi-io/dfe-receiver/commit/fabe429117b9381f509c68487505890725959d85))

# [1.14.0-dev.3](https://github.com/hyperi-io/dfe-receiver/compare/v1.14.0-dev.2...v1.14.0-dev.3) (2026-03-19)


### Bug Fixes

* use MemoryGuardConfig::from_env for standard env var overrides ([4a0b302](https://github.com/hyperi-io/dfe-receiver/commit/4a0b30281aca56df7e0bb626f92157d9c07c6d61))

# [1.14.0-dev.2](https://github.com/hyperi-io/dfe-receiver/compare/v1.14.0-dev.1...v1.14.0-dev.2) (2026-03-19)


### Bug Fixes

* clippy explicit_iter_loop in slowloris test ([c592019](https://github.com/hyperi-io/dfe-receiver/commit/c59201926160368fb8e3e1e46a4879320631922f))
* minor GA readiness items ([44840ec](https://github.com/hyperi-io/dfe-receiver/commit/44840ec5d62c10e682a016940a97af9d6c6bd602))

# [1.14.0-dev.1](https://github.com/hyperi-io/dfe-receiver/compare/v1.13.2-dev.3...v1.14.0-dev.1) (2026-03-19)


### Bug Fixes

* code review remediations ([94ca17c](https://github.com/hyperi-io/dfe-receiver/commit/94ca17c9cb2b086447398d8d1c1c3f1f4091a4ec))
* emit config_changed security event on pipeline config reload ([7133437](https://github.com/hyperi-io/dfe-receiver/commit/7133437d811b82e90f769556bab920a71545a8a4))
* integrate DfeMetrics from rustlib (dual-emit dfe_* alongside receiver_*) ([561e170](https://github.com/hyperi-io/dfe-receiver/commit/561e170902ef46257b9c5cc5dc75d20bc1771c3d))
* internet-facing hardening — slowloris, connection limits, rate limiting, IP filter ([ef7e8df](https://github.com/hyperi-io/dfe-receiver/commit/ef7e8df58b1738da39d0d782ba3159e6e59c51ed))
* remove [patch.crates-io], bump rustlib to >=1.16.3 (published) ([a5992eb](https://github.com/hyperi-io/dfe-receiver/commit/a5992eb6bdb263d1edabe0e964ca7bc3e8ef96ca))
* update KEDA PromQL to dfe_scaling_pressure ([631fd25](https://github.com/hyperi-io/dfe-receiver/commit/631fd2554f3974db2373a4d5ef6eb47cfcec9e3c))
* wire log spam helpers into identified hot spots ([fbea6f4](https://github.com/hyperi-io/dfe-receiver/commit/fbea6f42273c247e2e20eaf215e8af18ae30d2cf))
* wire security event logging into auth, TLS, and config reload ([2526c04](https://github.com/hyperi-io/dfe-receiver/commit/2526c04058f061b80a7adb7f8cc691444859ab72))


### Features

* add opt-in disk spillover via rustlib TieredSink ([5d798f6](https://github.com/hyperi-io/dfe-receiver/commit/5d798f620d82db85656950453bd3bc9570452e5a))
* add optional Prometheus scaling trigger to KEDA ScaledObject ([d3df594](https://github.com/hyperi-io/dfe-receiver/commit/d3df59449b41a5e4b3d58c2bc49aad12c76595fe))
* add RustlibSinkAdapter for bridging sink traits ([c71cb2f](https://github.com/hyperi-io/dfe-receiver/commit/c71cb2f3e183ecc7b6f79555cf9af1983ecabfad))
* wire SharedConfig hot-reload to auth state ([9f57c6e](https://github.com/hyperi-io/dfe-receiver/commit/9f57c6efdd261c61a39e99dbb8d525697e05cb2f))


### Performance Improvements

* expand benchmark suite with router and metrics render groups ([18c1705](https://github.com/hyperi-io/dfe-receiver/commit/18c1705ffe2ff1960721912d5d1eb0904b98478a))

## [1.13.2-dev.3](https://github.com/hyperi-io/dfe-receiver/compare/v1.13.2-dev.2...v1.13.2-dev.3) (2026-03-16)


### Bug Fixes

* disable rdkafka stats spam by default (closes [#3](https://github.com/hyperi-io/dfe-receiver/issues/3)) ([3654677](https://github.com/hyperi-io/dfe-receiver/commit/3654677f9fff782e6d19d3de90d820bbdb98c4e8))

## [1.13.2-dev.2](https://github.com/hyperi-io/dfe-receiver/compare/v1.13.2-dev.1...v1.13.2-dev.2) (2026-03-16)


### Bug Fixes

* correct port conflicts and add missing protocol ports ([7596bf0](https://github.com/hyperi-io/dfe-receiver/commit/7596bf0400f4161c7a4caeac0650742f874960dd))
* remove plugin system, document sidecar transport pattern ([4a453fb](https://github.com/hyperi-io/dfe-receiver/commit/4a453fba0d40eccc09a91998913dabe5ad4ed8e3))
* stabilise grpc tls test, enable r2 publishing ([ddbca53](https://github.com/hyperi-io/dfe-receiver/commit/ddbca53b6265dba53fc95dd3029e29c3c803946e))
* update docs for plugin removal and CI migration ([0173908](https://github.com/hyperi-io/dfe-receiver/commit/01739082ac31bfdbecd066cd4169f667109951d8))

## [1.13.2-dev.1](https://github.com/hyperi-io/dfe-receiver/compare/v1.13.1...v1.13.2-dev.1) (2026-03-12)


### Bug Fixes

* add build.type app, remove legacy publish workflow ([30e03df](https://github.com/hyperi-io/dfe-receiver/commit/30e03df5b16d0e367c5d774bc7deb8c54e8a64b8))
* consume hyperi-rustlib 1.16.0 dynamic linking ([6bb8ebd](https://github.com/hyperi-io/dfe-receiver/commit/6bb8ebda08308465687a8265601bd8a432c3e88d))
* update Dockerfile header and fix UID 1000 conflict [skip ci] ([434ce5c](https://github.com/hyperi-io/dfe-receiver/commit/434ce5ce546fdbbe9d34946b877557e7c3ba6250))

## [1.13.1](https://github.com/hyperi-io/dfe-receiver/compare/v1.13.0...v1.13.1) (2026-03-10)


### Bug Fixes

* add base_image field to DeploymentContract ([058201a](https://github.com/hyperi-io/dfe-receiver/commit/058201a33027482b4004da35e1f07a4da88b2b13))
* add Fluent Forward, GELF handlers and integration tests [skip ci] ([ab85ad2](https://github.com/hyperi-io/dfe-receiver/commit/ab85ad296dc91a6de4dc7e659f1ce37b612d1160))
* change default_source from "dfe" to "default" [skip ci] ([e3894cc](https://github.com/hyperi-io/dfe-receiver/commit/e3894cca7b8898adfd4fbdf128da241a1d57ff93))
* fmt and clippy fixes for Rust 1.94, update STATE.md for crates.io rustlib [skip ci] ([fc8326d](https://github.com/hyperi-io/dfe-receiver/commit/fc8326d040f61854acdf70631da45e968ab78fff))
* remove unknown cross build strategy from config ([294cff1](https://github.com/hyperi-io/dfe-receiver/commit/294cff1a1c3ce0579565d123578c22d168b98997))
* resolve all cargo clippy and fmt errors for hyperi-ci pipeline ([ce1aa40](https://github.com/hyperi-io/dfe-receiver/commit/ce1aa408b890d4161f6fff91d4d6837288970bcb))
* resolve Rust 2024 collapsible_if and feature flag errors ([89a762d](https://github.com/hyperi-io/dfe-receiver/commit/89a762d48c0121817d357dadaa4dbd71ef9580cd))
* suppress remaining pedantic clippy lints for hyperi-ci pipeline ([7ef2ce1](https://github.com/hyperi-io/dfe-receiver/commit/7ef2ce119cba99016391bfedc935ba69d239ff5f))
* switch hyperi-rustlib to crates.io, add base_image to DeploymentContract [skip ci] ([8b05762](https://github.com/hyperi-io/dfe-receiver/commit/8b05762f0e04ed3a9bb373abbfd347ece91acaa0))
* trigger CI release after dep upgrades and crates.io migration ([3cfdf97](https://github.com/hyperi-io/dfe-receiver/commit/3cfdf97459d1b501c0b86bad1ceabb345463c56a))
* upgrade deps to latest, edition 2024, migrate Kafka sinks to rustlib [skip ci] ([9d88ffc](https://github.com/hyperi-io/dfe-receiver/commit/9d88ffcb4281ef82efdfaf7412e89309ca584cb0))
* vendor google/protobuf/timestamp.proto for CI protoc compatibility ([cf08fca](https://github.com/hyperi-io/dfe-receiver/commit/cf08fca0023630ae4c5394b40a9a0c8c2ba6b2e8))

# [1.13.0](https://github.com/hyperi-io/dfe-receiver/compare/v1.12.1...v1.13.0) (2026-03-04)


### Bug Fixes

* exclude chart, scripts dirs from cargo publish package [skip ci] ([686b57b](https://github.com/hyperi-io/dfe-receiver/commit/686b57b219b4df5a84359f9d322b2854911e0c7f))


### Features

* add gRPC loader transport and file debug sink ([5279839](https://github.com/hyperi-io/dfe-receiver/commit/5279839f4bffb7ef023e31153a662db51654bd13))

## [1.12.1](https://github.com/hyperi-io/dfe-receiver/compare/v1.12.0...v1.12.1) (2026-03-04)


### Bug Fixes

* update ci submodule with test parallelism fix [skip ci] ([c9a785d](https://github.com/hyperi-io/dfe-receiver/commit/c9a785dee526715ef9958b0889ae9eaba987062c))
* use uid 10001 for appuser to avoid collision with ubuntu user in base image ([83d8040](https://github.com/hyperi-io/dfe-receiver/commit/83d8040af8e88fab732cd6ccde8e3a4e9b4b69b2))

# [1.12.0](https://github.com/hyperi-io/dfe-receiver/compare/v1.11.0...v1.12.0) (2026-03-04)


### Features

* enable container and Helm publishing with multi-arch Dockerfile ([5456936](https://github.com/hyperi-io/dfe-receiver/commit/54569364a7ab793d4db832f38ea2a4db2b141156))

# [1.11.0](https://github.com/hyperi-io/dfe-receiver/compare/v1.10.3...v1.11.0) (2026-03-03)


### Bug Fixes

* add otel/hyperdx output modes to prometheus remote write ([72fb5de](https://github.com/hyperi-io/dfe-receiver/commit/72fb5de5352ce038941fc8db0ea7cdcaf1e4beb1))
* wire rustlib cli/deployment module and generate artefacts ([1108f47](https://github.com/hyperi-io/dfe-receiver/commit/1108f4717dbfb79df96a354ff36052a4131958bf))


### Features

* add prometheus remote write v1 receiver ([645844f](https://github.com/hyperi-io/dfe-receiver/commit/645844f657e18b396935829d95504dcc74163bf4))
* add syslog protocol handler (UDP + TCP + TLS) ([b6f1651](https://github.com/hyperi-io/dfe-receiver/commit/b6f165113b8f679b6188d49a8c72c694550477f9))

## [1.10.3](https://github.com/hyperi-io/dfe-receiver/compare/v1.10.2...v1.10.3) (2026-03-03)


### Bug Fixes

* update ci submodule with publish-binary source order fix ([57d23ae](https://github.com/hyperi-io/dfe-receiver/commit/57d23ae4c1ae6f0dda6c37fd86dc7994fbd7e007))

## [1.10.2](https://github.com/hyperi-io/dfe-receiver/compare/v1.10.1...v1.10.2) (2026-03-03)


### Bug Fixes

* update ci submodule with aarch64 cross-compile linker fix ([c34018b](https://github.com/hyperi-io/dfe-receiver/commit/c34018b32e7ef4c958060f38da660b673eb0d2c5))

## [1.10.1](https://github.com/hyperi-io/dfe-receiver/compare/v1.10.0...v1.10.1) (2026-03-03)


### Bug Fixes

* resolve clippy errors in lumberjack codec and splunk hec handler ([cdf1ca9](https://github.com/hyperi-io/dfe-receiver/commit/cdf1ca94b99b4ef814a6bcdaff443755cda9ba5e))

# [1.10.0](https://github.com/hyperi-io/dfe-receiver/compare/v1.9.4...v1.10.0) (2026-03-03)


### Features

* add Lumberjack v2 (Beats) protocol handler with Filebeat integration tests ([7bdc9fb](https://github.com/hyperi-io/dfe-receiver/commit/7bdc9fb3604888c30a0ab9f3b122834d5347ccfe))
* add Splunk HEC protocol handler with integration tests ([a729f81](https://github.com/hyperi-io/dfe-receiver/commit/a729f81bd1dfaf304c4f379ca2edcd67223a856b))

## [1.9.4](https://github.com/hyperi-io/dfe-receiver/compare/v1.9.3...v1.9.4) (2026-03-02)


### Bug Fixes

* replace hard-coded DLQ routing with unified rustlib dlq module ([13c46a2](https://github.com/hyperi-io/dfe-receiver/commit/13c46a218855ba0f9c8070847ea99f1fff7e921e))

## [1.9.3](https://github.com/hyperi-io/dfe-receiver/compare/v1.9.2...v1.9.3) (2026-03-02)


### Bug Fixes

* auto-download vector binary for integration tests ([8556d8f](https://github.com/hyperi-io/dfe-receiver/commit/8556d8fe0381360e646b84385da4a0593741d3df))
* migrate scaling metric to rustlib ScalingPressure engine ([786fa7c](https://github.com/hyperi-io/dfe-receiver/commit/786fa7c1ec70f5def75aafab2b338fe85f05ad11))
* wire up KEDA scaling metric with gated composite logic ([2e12d59](https://github.com/hyperi-io/dfe-receiver/commit/2e12d59019cbbe9cac7c6ec57d6a3d93d0f4e295))

## [1.9.2](https://github.com/hyperi-io/dfe-receiver/compare/v1.9.1...v1.9.2) (2026-03-02)


### Bug Fixes

* cargo fmt formatting ([8d0c8ff](https://github.com/hyperi-io/dfe-receiver/commit/8d0c8ffb86ed2cab4a2a1ebfc300a8dbb195339e))

## [1.9.1](https://github.com/hyperi-io/dfe-receiver/compare/v1.9.0...v1.9.1) (2026-03-02)


### Bug Fixes

* update rustlib to v1.8.1, update ci/ai submodules, fix RwLock access ([a338d25](https://github.com/hyperi-io/dfe-receiver/commit/a338d2561a8679de31923afc84e82bfba28bec90))

# [1.9.0](https://github.com/hyperi-io/dfe-receiver/compare/v1.8.7...v1.9.0) (2026-02-25)


### Features

* SharedConfig, env overrides, config reload, serde_yaml_ng migration ([a9fbd8f](https://github.com/hyperi-io/dfe-receiver/commit/a9fbd8f74a040481140439d0fefc6f0d8615e65d))
* source-rule routing, timestamp enrichment, config refresh ([0fe325a](https://github.com/hyperi-io/dfe-receiver/commit/0fe325ab215570e5c5b63dacfe1611df01f9889d))

## [1.8.7](https://github.com/hyperi-io/dfe-receiver/compare/v1.8.6...v1.8.7) (2026-02-25)


### Bug Fixes

* Use new version of plugin loader and no need for deps dir ([73ab50c](https://github.com/hyperi-io/dfe-receiver/commit/73ab50caed325e66198c0966ea66df00bbac80c4))

## [1.8.6](https://github.com/hyperi-io/dfe-receiver/compare/v1.8.5...v1.8.6) (2026-02-24)


### Bug Fixes

* Update the version constraint ([5c456c7](https://github.com/hyperi-io/dfe-receiver/commit/5c456c79e86f326aea1b0446e6c5f00f5d421f6c))

## [1.8.5](https://github.com/hyperi-io/dfe-receiver/compare/v1.8.4...v1.8.5) (2026-02-24)


### Bug Fixes

* Using symlinks for dependency repos ([9733c12](https://github.com/hyperi-io/dfe-receiver/commit/9733c12eb73472c5c332acbabaf7531fb118920e))

## [1.8.4](https://github.com/hyperi-io/dfe-receiver/compare/v1.8.3...v1.8.4) (2026-02-24)


### Bug Fixes

* Checkout the proper repos instead of stubs ([f22608c](https://github.com/hyperi-io/dfe-receiver/commit/f22608c39abaa09e35903c84e73e90d096627186))

## [1.8.3](https://github.com/hyperi-io/dfe-receiver/compare/v1.8.2...v1.8.3) (2026-02-24)


### Bug Fixes

* More stub crates needed ([01f345e](https://github.com/hyperi-io/dfe-receiver/commit/01f345e516c09bd5f21508c9405f37af1bc14013))

## [1.8.2](https://github.com/hyperi-io/dfe-receiver/compare/v1.8.1...v1.8.2) (2026-02-24)


### Bug Fixes

* Create stub crates for Cargo to work ([213baff](https://github.com/hyperi-io/dfe-receiver/commit/213baffedbbb1b1dbd153541eda55cd12ca6e495))

## [1.8.1](https://github.com/hyperi-io/dfe-receiver/compare/v1.8.0...v1.8.1) (2026-02-24)


### Bug Fixes

* Allow explicit config file specification for config ([2c68d5c](https://github.com/hyperi-io/dfe-receiver/commit/2c68d5ca3fc94c1fa61064b13e1f6af8b214d249))

# [1.8.0](https://github.com/hyperi-io/dfe-receiver/compare/v1.7.0...v1.8.0) (2026-02-20)


### Features

* add dynamic plugin system with C ABI loader ([521849c](https://github.com/hyperi-io/dfe-receiver/commit/521849cdce12346712bf756f31772cbad9cbfd37))

# [1.7.0](https://github.com/hyperi-io/dfe-receiver/compare/v1.6.3...v1.7.0) (2026-02-19)


### Features

* add OTLP protocol support with dual-mode conversion ([38d98cb](https://github.com/hyperi-io/dfe-receiver/commit/38d98cb90e66fdc11af4f363f905651fda9a3eb0))

## [1.6.3](https://github.com/hyperi-io/dfe-receiver/compare/v1.6.2...v1.6.3) (2026-02-17)


### Bug Fixes

* use standard runner for release workflow instead of buildjet ([ccdc924](https://github.com/hyperi-io/dfe-receiver/commit/ccdc9248d6c72cd46e51d04b854679127e96e311))

## [1.6.2](https://github.com/hyperi-io/dfe-receiver/compare/v1.6.1...v1.6.2) (2026-02-17)


### Bug Fixes

* resolve clippy approx_constant and cargo fmt issues ([03a6887](https://github.com/hyperi-io/dfe-receiver/commit/03a688778a70a5a548aba9512b1138eebec0ac5e))

## [1.6.1](https://github.com/hyperi-io/dfe-receiver/compare/v1.6.0...v1.6.1) (2026-02-17)


### Bug Fixes

* use non-approx-constant float in test to satisfy clippy ([d22cfcc](https://github.com/hyperi-io/dfe-receiver/commit/d22cfccca77cb2b6b3fd066683da31f5f73f925b))

# [1.6.0](https://github.com/hyperi-io/dfe-receiver/compare/v1.5.0...v1.6.0) (2026-02-17)


### Features

* add grpc vector protocol, tls hot-reload, bearer auth tests and rebrand fixes ([c668797](https://github.com/hyperi-io/dfe-receiver/commit/c66879702ff23bc65bd3df19d8c3602b4af774bf))

# [1.5.0](https://github.com/hypersec-io/dfe-receiver/compare/v1.4.11...v1.5.0) (2026-02-03)


### Features

* configure rust feature sets for CI testing ([953248e](https://github.com/hypersec-io/dfe-receiver/commit/953248eaa5680c698fc1d307f027de2073be9840))

## [1.4.11](https://github.com/hypersec-io/dfe-receiver/compare/v1.4.10...v1.4.11) (2026-02-03)


### Bug Fixes

* update benchmark to use sonic_rs::from_slice API ([b02ad26](https://github.com/hypersec-io/dfe-receiver/commit/b02ad261074a1446d5ba7e81e0c5ee02ec305632))

## [1.4.10](https://github.com/hypersec-io/dfe-receiver/compare/v1.4.9...v1.4.10) (2026-02-03)


### Bug Fixes

* allow unwrap/expect in test code ([aeca759](https://github.com/hypersec-io/dfe-receiver/commit/aeca759ac98e4aea0f9bd264bc5d1ca2b51a5ae1))

## [1.4.9](https://github.com/hypersec-io/dfe-receiver/compare/v1.4.8...v1.4.9) (2026-02-03)


### Bug Fixes

* update ci submodule for Cargo.toml lint support ([353488d](https://github.com/hypersec-io/dfe-receiver/commit/353488d445d80db6026025e1745d39dc0243df53))

## [1.4.8](https://github.com/hypersec-io/dfe-receiver/compare/v1.4.7...v1.4.8) (2026-02-03)


### Bug Fixes

* configure clippy to allow more pedantic lints during development ([0cdcd12](https://github.com/hypersec-io/dfe-receiver/commit/0cdcd129d7a080d956e8b06e4b7e00c6d9a92541))

## [1.4.7](https://github.com/hypersec-io/dfe-receiver/compare/v1.4.6...v1.4.7) (2026-02-03)


### Bug Fixes

* add typos.toml to configure spell checker ([f54e349](https://github.com/hypersec-io/dfe-receiver/commit/f54e3492c665f0018cdfd6eac60baa46278147ec))

## [1.4.6](https://github.com/hypersec-io/dfe-receiver/compare/v1.4.5...v1.4.6) (2026-02-03)


### Bug Fixes

* allow clippy lints in test modules for unwrap and format helpers ([74db14e](https://github.com/hypersec-io/dfe-receiver/commit/74db14ee14cb2b7504361d2b8405ae5cc10ff537))

## [1.4.5](https://github.com/hypersec-io/dfe-receiver/compare/v1.4.4...v1.4.5) (2026-02-03)


### Bug Fixes

* allow clippy format lints in test modules ([4795e62](https://github.com/hypersec-io/dfe-receiver/commit/4795e62d3dd037679251b757a67a6f85d659ee99))

## [1.4.4](https://github.com/hypersec-io/dfe-receiver/compare/v1.4.3...v1.4.4) (2026-02-03)


### Bug Fixes

* update ci submodule with libcurl fix ([4512424](https://github.com/hypersec-io/dfe-receiver/commit/4512424df122e1d40d95122b19b040a555fc55ea))

## [1.4.3](https://github.com/hypersec-io/dfe-receiver/compare/v1.4.2...v1.4.3) (2026-02-03)


### Bug Fixes

* update ci submodule to latest ([c649ae5](https://github.com/hypersec-io/dfe-receiver/commit/c649ae53f665efe91f120ff67416c4747b4b9fa1))

## [1.4.2](https://github.com/hypersec-io/dfe-receiver/compare/v1.4.1...v1.4.2) (2026-02-03)


### Bug Fixes

* **ci:** exclude non-source directories from cargo package ([00d8447](https://github.com/hypersec-io/dfe-receiver/commit/00d84479f20bddf9795465282bb43813504b0d08))

## [1.4.1](https://github.com/hypersec-io/dfe-receiver/compare/v1.4.0...v1.4.1) (2026-02-03)


### Bug Fixes

* **ci:** add --allow-dirty flag for cargo publish ([130b4c9](https://github.com/hypersec-io/dfe-receiver/commit/130b4c9c2d87c9852fc9e6ffd5ea4d564731c514))

# [1.4.0](https://github.com/hypersec-io/dfe-receiver/compare/v1.3.4...v1.4.0) (2026-02-03)


### Features

* **deps:** switch hs-rustlib from git to JFrog Cargo registry ([c8c1c3e](https://github.com/hypersec-io/dfe-receiver/commit/c8c1c3e3abc9c43e6f1a268387c63bdc8785995e))

## [1.3.4](https://github.com/hypersec-io/dfe-receiver/compare/v1.3.3...v1.3.4) (2026-02-03)


### Bug Fixes

* resolve allocator conflict when both jemalloc and mimalloc features enabled ([5ce709d](https://github.com/hypersec-io/dfe-receiver/commit/5ce709dffed53581ab253748c7bad7be3240d913))

## [1.3.3](https://github.com/hypersec-io/dfe-receiver/compare/v1.3.2...v1.3.3) (2026-02-03)


### Bug Fixes

* **ci:** add libcurl-dev for rdkafka build in release workflow ([5af1cbe](https://github.com/hypersec-io/dfe-receiver/commit/5af1cbe43bc2046acee3f89b998037f399565117))

## [1.3.2](https://github.com/hypersec-io/dfe-receiver/compare/v1.3.1...v1.3.2) (2026-02-03)


### Bug Fixes

* **ci:** add GitHub App token for private repo access in release workflow ([81cfcda](https://github.com/hypersec-io/dfe-receiver/commit/81cfcda986b0b3e4844430aaf98d1e2cb6ff1e44))

## [1.3.1](https://github.com/hypersec-io/dfe-receiver/compare/v1.3.0...v1.3.1) (2026-02-03)


### Bug Fixes

* resolve gitleaks false positive on README example token ([c823246](https://github.com/hypersec-io/dfe-receiver/commit/c8232467f949aeeabbe7a3892296991703147b64))

# [1.3.0](https://github.com/hypersec-io/dfe-receiver/compare/v1.2.0...v1.3.0) (2026-02-03)


### Features

* **security:** add HTTP security hardening and Rust CI ([a4a08c7](https://github.com/hypersec-io/dfe-receiver/commit/a4a08c748e98b930f78c38b02d42ba35aa89564b))

# [1.2.0](https://github.com/hypersec-io/dfe-receiver/compare/v1.1.0...v1.2.0) (2026-02-03)


### Features

* TLS from secrets + remove disk spillover ([0045f98](https://github.com/hypersec-io/dfe-receiver/commit/0045f98e44eb9136613b6a555af779af0a0a78ee))

# [1.1.0](https://github.com/hypersec-io/dfe-receiver/compare/v1.0.0...v1.1.0) (2026-02-03)


### Features

* **auth:** add bearer token authentication with secret manager support ([cc65d2a](https://github.com/hypersec-io/dfe-receiver/commit/cc65d2ade219ef6d7996dcbc518091e1850940b7))

# 1.0.0 (2026-02-03)


### Features

* initial dfe-receiver implementation ([7ddd7a9](https://github.com/hypersec-io/dfe-receiver/commit/7ddd7a985ca2e74c997568c96997663ebb6f662e)), closes [Hi#performance](https://github.com/Hi/issues/performance)
