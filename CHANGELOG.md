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
