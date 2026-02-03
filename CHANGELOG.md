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
