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
