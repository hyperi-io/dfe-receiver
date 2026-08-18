# Changelog

Rendered by CI and committed back at the end of a release -- do not edit by
hand. Release notes also appear on the GitHub Releases page, one per tag.

## [1.15.23](https://github.com/hyperi-io/dfe-receiver/compare/v1.15.22...v1.15.23) (2026-08-18)

## [1.15.22](https://github.com/hyperi-io/dfe-receiver/compare/v1.15.21...v1.15.22) (2026-08-18)

## [1.15.21](https://github.com/hyperi-io/dfe-receiver/compare/v1.15.20...v1.15.21) (2026-08-18)

## [1.15.20](https://github.com/hyperi-io/dfe-receiver/compare/v1.15.19...v1.15.20) (2026-08-17)

## [1.15.19](https://github.com/hyperi-io/dfe-receiver/compare/v1.15.18...v1.15.19) (2026-08-04)

## [1.15.18](https://github.com/hyperi-io/dfe-receiver/compare/v1.15.17...v1.15.18) (2026-08-03)

# 1.0.0 (2026-06-01)


### Bug Fixes

* add 3-mode envelope renderer (canonical, canonical_with_raw, exploded) ([84e0a26](https://github.com/hyperi-io/dfe-receiver/commit/84e0a26e120cfb7e73d372292dfaeb0d8924e506))
* add base_image field to DeploymentContract ([c58b25a](https://github.com/hyperi-io/dfe-receiver/commit/c58b25a5f392a9c02dc23553ab8593592f69ae9c))
* add build.type app, remove legacy publish workflow ([40ff337](https://github.com/hyperi-io/dfe-receiver/commit/40ff3376b9be33968368554562cc8bd2ea225f91))
* add canonical flow + counter record schema (incl NSEL and NAT44 fields) ([a288008](https://github.com/hyperi-io/dfe-receiver/commit/a28800838d8cca680d59bae8497a59ca9cce02e7))
* add criterion benches for flow decode + envelope ([c6ef5f0](https://github.com/hyperi-io/dfe-receiver/commit/c6ef5f025edb4646ee07ac2b407276f951377fc4))
* add debug and trace logging for request handling, routing, and sinks ([bc177eb](https://github.com/hyperi-io/dfe-receiver/commit/bc177eb70d7345b99886ec5f9d0d20e1f501541e))
* add end-to-end integration tests for NetFlow + sFlow via testcontainers ([b4bee62](https://github.com/hyperi-io/dfe-receiver/commit/b4bee6258d794569c921e4c0b412113e710add51))
* add flow config types (unified + split modes, experimental marker) ([8d463a7](https://github.com/hyperi-io/dfe-receiver/commit/8d463a7e0283bc36426891c10abb6b397d58eb78))
* add flow protocol-kind dispatch + length sanity checks ([d4cdf7f](https://github.com/hyperi-io/dfe-receiver/commit/d4cdf7f2a41349571a7636ff5377c2af2a7b6e0c))
* add FlowDecoder trait + DecodedPacket struct ([2f66930](https://github.com/hyperi-io/dfe-receiver/commit/2f669303687cfd7f5a999798f230da36886bca98))
* add FlowHandler (unified + split modes) impl ProtocolHandler ([eec75e6](https://github.com/hyperi-io/dfe-receiver/commit/eec75e6f68bb9be32d14ac97a19b2ce8f62b8cbe))
* add Fluent Forward, GELF handlers and integration tests [skip ci] ([125dcbd](https://github.com/hyperi-io/dfe-receiver/commit/125dcbd4dab8d73f120357da77379f9ce96d9251))
* add gitignore entry to trigger CI for semantic-release ([415cbdd](https://github.com/hyperi-io/dfe-receiver/commit/415cbddcb563fc3780fa17fc80676118870f010e))
* add NetflowDecoder with hand-rolled v5 + netgauze v9/IPFIX wiring ([818d948](https://github.com/hyperi-io/dfe-receiver/commit/818d94813cda128481b57ede6d1eb52d4dd2199b))
* add netgauze, nom, socket2, dashmap deps for flow handler ([147fa9c](https://github.com/hyperi-io/dfe-receiver/commit/147fa9c9c7fd89f2e37b3c36c077838b1ad10c9e))
* add otel/hyperdx output modes to prometheus remote write ([463d9fa](https://github.com/hyperi-io/dfe-receiver/commit/463d9fa2cc096db4506c4c141e328336899b07e6))
* add per-source-IP rate limiter (DashMap + atomic bucket) ([27409fd](https://github.com/hyperi-io/dfe-receiver/commit/27409fd1174d60a4f6a7fc1f82b68975847561ca))
* add process_batch() for multi-message handler batching ([0fe4047](https://github.com/hyperi-io/dfe-receiver/commit/0fe40474862e5e0039f1a6dc7b85d36bd4a5efa9))
* add proptest fuzz harness for NetflowDecoder + SflowDecoder ([cf4972f](https://github.com/hyperi-io/dfe-receiver/commit/cf4972f7356b8dffd355e5d39aabbad5ad849ed7))
* add real-world PCAP corpus + integration tests for flow decoders ([3cb2b1c](https://github.com/hyperi-io/dfe-receiver/commit/3cb2b1c78b8c5a65fa9c145eaebbbfb5a2f34ec6))
* add request duration histogram, active connections gauge, hot path optimisations ([9aea116](https://github.com/hyperi-io/dfe-receiver/commit/9aea116580058fcbfc14aca736b32a41dde9901c))
* add sFlow v5 decoder (nom parser + canonical mapping + sampled-IP walker) ([269fdf7](https://github.com/hyperi-io/dfe-receiver/commit/269fdf731e733a3552014c38cbc8e364f31550ba))
* add template_miss test documenting netgauze 0.12 limitation ([2817d59](https://github.com/hyperi-io/dfe-receiver/commit/2817d5910ce9128e58bfd3e8a3201cc0fc7aae6a))
* add typos.toml to configure spell checker ([ccf689d](https://github.com/hyperi-io/dfe-receiver/commit/ccf689d88d11f81c83b26ee6b61c09d9ef220954))
* add UDP flow listener with gates, recvmmsg fallback, kernel-drop polling ([5292b9b](https://github.com/hyperi-io/dfe-receiver/commit/5292b9b0f77bcda8d8cf7a4bea9ab9c66c6f355b))
* add version check on startup, document crates.io-only rustlib rule ([df5fddf](https://github.com/hyperi-io/dfe-receiver/commit/df5fddf44d3c63698422dd8fd863743cb3cf3bd2))
* add worker feature for future parallel batch validation ([0733076](https://github.com/hyperi-io/dfe-receiver/commit/0733076721d4a6cdd0dc07e093f27802b6417069))
* adopt v2.7.1 DLQ API (Dlq::spawn + queue-admission send semantics) ([bf3d39d](https://github.com/hyperi-io/dfe-receiver/commit/bf3d39dde55c0572b52bf37e0248f508c3e44c57))
* align VERSION file with latest release tag ([4677649](https://github.com/hyperi-io/dfe-receiver/commit/46776498eb707a7b90671d5262972a5e06e33396))
* allow clippy format lints in test modules ([96165f3](https://github.com/hyperi-io/dfe-receiver/commit/96165f347b3d012ae1dd59628cbcc4a61b139289))
* allow clippy lints in test modules for unwrap and format helpers ([dcbbbe4](https://github.com/hyperi-io/dfe-receiver/commit/dcbbbe47f3c5e04dbd55296174f6117e8aa557ff))
* Allow explicit config file specification for config ([075da34](https://github.com/hyperi-io/dfe-receiver/commit/075da34642c97943a7871d1eb0676f83e80513b8))
* allow unwrap/expect in test code ([5548b74](https://github.com/hyperi-io/dfe-receiver/commit/5548b747f06b2582a238654b6f6a40ef1444f129))
* auto-download vector binary for integration tests ([540a3e4](https://github.com/hyperi-io/dfe-receiver/commit/540a3e4efa50299a16335f6922ebd6c35718deef))
* bump hyperi-rustlib to >=2.4.3 and add DeploymentContract fields ([8ed1456](https://github.com/hyperi-io/dfe-receiver/commit/8ed14567d3e17803bbeb4588ba8f979ad2ee7e7a))
* bump hyperi-rustlib to >=2.5.4, handle FilteredDlq variant ([00392c4](https://github.com/hyperi-io/dfe-receiver/commit/00392c46c4c4a1d93d6717174314f6986a3e37f1))
* canary release through BOLT + R2 on ARC runner v1.12.1 ([1ae5a9d](https://github.com/hyperi-io/dfe-receiver/commit/1ae5a9d62ef05eabc38d5ca06d63a50894102a2d))
* cargo fmt formatting ([63fe3d1](https://github.com/hyperi-io/dfe-receiver/commit/63fe3d126944fb1cfc0659143b1d6956a2b969c2))
* change default_source from "dfe" to "default" [skip ci] ([faf406b](https://github.com/hyperi-io/dfe-receiver/commit/faf406b9f99f3edc31d88140a2838cf5604240a8))
* Checkout the proper repos instead of stubs ([61d8742](https://github.com/hyperi-io/dfe-receiver/commit/61d87421e284ee3b45c9351a3d9596ce14e4893e))
* **ci:** add --allow-dirty flag for cargo publish ([5bb0ef2](https://github.com/hyperi-io/dfe-receiver/commit/5bb0ef2be91161206be7826fb6a498b333d0e26d))
* **ci:** add GitHub App token for private repo access in release workflow ([fc0939e](https://github.com/hyperi-io/dfe-receiver/commit/fc0939ead5556f0a88426aef5ec943b6c32f6d14))
* **ci:** add libcurl-dev for rdkafka build in release workflow ([35ec7fb](https://github.com/hyperi-io/dfe-receiver/commit/35ec7fbe23b91d22eeac176ff9cc8acf1f3460f1))
* **ci:** bump hyperi-ci reusable workflow pin to v2.6.4 ([1132feb](https://github.com/hyperi-io/dfe-receiver/commit/1132febcfa5c1e18536fad076fb69171ecbd5526))
* **ci:** exclude non-source directories from cargo package ([08ff37d](https://github.com/hyperi-io/dfe-receiver/commit/08ff37d971aa89dea48a6bb3f2aafe69bc49a096))
* **ci:** swap PGO workload Kafka JVM for Redpanda (closes [#34](https://github.com/hyperi-io/dfe-receiver/issues/34)) ([8e6704a](https://github.com/hyperi-io/dfe-receiver/commit/8e6704a0ecf9e54abb896d5820a869028bbfb256))
* **ci:** unpin hyperi-ci workflow from v1.5.0 digest to [@main](https://github.com/main) ([27da481](https://github.com/hyperi-io/dfe-receiver/commit/27da481acc97ded8b3c7b7c0c1ffe51400b1c582)), closes [#27](https://github.com/hyperi-io/dfe-receiver/issues/27)
* cleanup pre-existing expect()-on-Option + rustfmt ([f0d3bc4](https://github.com/hyperi-io/dfe-receiver/commit/f0d3bc4943714105f90153a2c12412ecde003eb8))
* **cli:** align with dfe-loader StandardCommand pattern ([2b8b69d](https://github.com/hyperi-io/dfe-receiver/commit/2b8b69d2b29fdc03164692e12c89c66ebdfcf811))
* clippy explicit_iter_loop in slowloris test ([62ab346](https://github.com/hyperi-io/dfe-receiver/commit/62ab346a068b2a72a641f69e1ac048dbdbb831c6))
* clippy field_reassign_with_default and expect_used in tests ([40feaaf](https://github.com/hyperi-io/dfe-receiver/commit/40feaafc023e318e41eefefcfeb11ffca5367460))
* clippy lints introduced in Rust 1.95 ([6676805](https://github.com/hyperi-io/dfe-receiver/commit/6676805784550f150aa24c3d40bc5f40244a6b64))
* clippy pedantic + rust 1.95 lints across flow modules and tests ([16773b2](https://github.com/hyperi-io/dfe-receiver/commit/16773b285cbdf13e5fe69fb508431897cef0c2bd))
* code review remediations ([74cf826](https://github.com/hyperi-io/dfe-receiver/commit/74cf826c131a9e0179f939522ad120f7b2c6efe2))
* complete v2.7.1 DLQ API migration in test files ([d35b03a](https://github.com/hyperi-io/dfe-receiver/commit/d35b03ad24469a30a106920ee4277282f76e7bdc))
* complete v2.7.1 DLQ migration — runtime + fmt fixups ([0b73f3c](https://github.com/hyperi-io/dfe-receiver/commit/0b73f3c13adfefacb68607af1a1058477fef8f0c))
* configure clippy to allow more pedantic lints during development ([7e91107](https://github.com/hyperi-io/dfe-receiver/commit/7e9110704df6b28d6ac5f9747dba8f09a216227f))
* consume hyperi-rustlib 1.16.0 dynamic linking ([4cdd75c](https://github.com/hyperi-io/dfe-receiver/commit/4cdd75cce378b079aab68f8e7f6ce4d53b131793))
* correct port conflicts and add missing protocol ports ([e649037](https://github.com/hyperi-io/dfe-receiver/commit/e649037f3f61141cfc75fb76e39baafe4939629a))
* Create stub crates for Cargo to work ([047a467](https://github.com/hyperi-io/dfe-receiver/commit/047a467b6068cd6602baa69a780180cdfcf926ea))
* deployment contract -- expose UDP 2055/4739/6343 + Helm chart flow section ([f32710b](https://github.com/hyperi-io/dfe-receiver/commit/f32710b4ee93e365f007be173c76e8a0ad3a2a23))
* **deployment:** wire DfeApp::deployment_contract trait hook + bump rustlib to >=2.7.0 ([6b883e3](https://github.com/hyperi-io/dfe-receiver/commit/6b883e3c187e030c1a054dcee57ec8f5e3f3bc13))
* **deps:** bump hyperi-rustlib to >=2.7.1 ([d7f2329](https://github.com/hyperi-io/dfe-receiver/commit/d7f23297bdbf31af1859c859dca6531ce46e45fc))
* **deps:** bump hyperi-rustlib to >=2.8.0 ([74700db](https://github.com/hyperi-io/dfe-receiver/commit/74700dbcb86c083c6e84ea213c83011cdc787da3))
* **deps:** bump hyperi-rustlib to >=2.8.3 ([cddec4f](https://github.com/hyperi-io/dfe-receiver/commit/cddec4f1ff9eca77a0c0052e2583927d887b4bf0))
* **deps:** patch 5 Dependabot advisories in transitive deps ([ca2e515](https://github.com/hyperi-io/dfe-receiver/commit/ca2e515b0fb962276aebb695978a58ee1d4035a6))
* **deps:** track rustlib 2.6.1 (cli→cli-service, worker→worker-pool) ([340e3a8](https://github.com/hyperi-io/dfe-receiver/commit/340e3a8841d7544a559fe5eabaf9a26fd8f04720))
* disable rdkafka stats spam by default (closes [#3](https://github.com/hyperi-io/dfe-receiver/issues/3)) ([3ffc0dc](https://github.com/hyperi-io/dfe-receiver/commit/3ffc0dc416fafea2d14d7acf46b19e0098ecb59c))
* emit config_changed security event on pipeline config reload ([caca38b](https://github.com/hyperi-io/dfe-receiver/commit/caca38b87927d07a1434dce83969e68dd8b02e24))
* emit template metrics + Helm split mode + e2e coverage (review [#3](https://github.com/hyperi-io/dfe-receiver/issues/3)-9) ([3fea460](https://github.com/hyperi-io/dfe-receiver/commit/3fea460d7abee39e911bbee4e8a6183ff9c6ef18)), closes [#3-9](https://github.com/hyperi-io/dfe-receiver/issues/3-9) [#4](https://github.com/hyperi-io/dfe-receiver/issues/4) [#5](https://github.com/hyperi-io/dfe-receiver/issues/5) [#7](https://github.com/hyperi-io/dfe-receiver/issues/7) [#8](https://github.com/hyperi-io/dfe-receiver/issues/8) [#9](https://github.com/hyperi-io/dfe-receiver/issues/9)
* exclude chart, scripts dirs from cargo publish package [skip ci] ([9489035](https://github.com/hyperi-io/dfe-receiver/commit/94890359fb505e338c0ff157afec75a3069ba6a6))
* exploded mode all-or-nothing emission (review [#2](https://github.com/hyperi-io/dfe-receiver/issues/2)) ([6d75820](https://github.com/hyperi-io/dfe-receiver/commit/6d758207a9decc2c9a1e12efa4a7ea91beb0e23b))
* extend PGO workload with NetFlow + sFlow traffic generators ([bd300f1](https://github.com/hyperi-io/dfe-receiver/commit/bd300f169d7116ff38ac7f8580916e0175afcddd))
* fmt and clippy fixes for Rust 1.94, update for crates.io rustlib [skip ci] ([1534632](https://github.com/hyperi-io/dfe-receiver/commit/15346320d68c3c60d591fb2e9f49e9899c2efda8))
* force CI for semantic-release — process_batch integration ([204f1e4](https://github.com/hyperi-io/dfe-receiver/commit/204f1e423afd8c58eaabaa6c669bc83b6107730b))
* inline Renovate config (preset resolution broken) ([cce128c](https://github.com/hyperi-io/dfe-receiver/commit/cce128c4958e7ddffbd1f4f6f0b5d03b7512687d))
* integrate DfeMetrics from rustlib (dual-emit dfe_* alongside receiver_*) ([fb187f0](https://github.com/hyperi-io/dfe-receiver/commit/fb187f0b971fbc0e403a1197e5923a1ee6754cfe))
* internet-facing hardening — slowloris, connection limits, rate limiting, IP filter ([8676c79](https://github.com/hyperi-io/dfe-receiver/commit/8676c7964186511db49d01c95527dadd58bf714c))
* **license:** finish BUSL migration in releaserc + ci.yml headers ([503bcb1](https://github.com/hyperi-io/dfe-receiver/commit/503bcb1d0115583f61d04b02dc0dcb7db37e3fcf)), closes [#36](https://github.com/hyperi-io/dfe-receiver/issues/36)
* **license:** relicense FSL-1.1-ALv2 -> BUSL-1.1 ([337f398](https://github.com/hyperi-io/dfe-receiver/commit/337f398847be800754f473a9d1d8e03830027bb2)), closes [#36](https://github.com/hyperi-io/dfe-receiver/issues/36)
* **license:** update .hyperi-ci.yaml licence header to BUSL-1.1 ([0b32828](https://github.com/hyperi-io/dfe-receiver/commit/0b328282d3f097d6c209f6e5ab136b3126039b12))
* migrate scaling metric to rustlib ScalingPressure engine ([8736032](https://github.com/hyperi-io/dfe-receiver/commit/87360322bff71337f330978216d67dc77160dbf6))
* migrate to hyperi-rustlib v1.20.0 transport trait split ([f0248c8](https://github.com/hyperi-io/dfe-receiver/commit/f0248c80e8665a5938f68da91e2694641321c00e))
* migrate to single versioning on main ([b970a0a](https://github.com/hyperi-io/dfe-receiver/commit/b970a0a8808105b5de416dcded45e7751a3660ca))
* minor GA readiness items ([04453d1](https://github.com/hyperi-io/dfe-receiver/commit/04453d126b1fbe9a82410400e531d1a1ce88b3b2))
* Mismatched type error ([80ffb39](https://github.com/hyperi-io/dfe-receiver/commit/80ffb39dc89feef6539741318391d70140596ac6))
* More stub crates needed ([4772649](https://github.com/hyperi-io/dfe-receiver/commit/477264997d742f7f8537a2633e6a212439c13996))
* NetFlow v9/IPFIX canonical mapping with NSEL + NAT44 discriminators ([5a6700b](https://github.com/hyperi-io/dfe-receiver/commit/5a6700bde85d979812381b736819d6475ebd7bf9))
* opt in to hyperi-ci Tier 2 PGO + BOLT on release channel ([07b5d91](https://github.com/hyperi-io/dfe-receiver/commit/07b5d91fd2b989e156c763a8e4fecad925767ee7))
* **pgo:** build pgo-driver on-demand during workload orchestration ([c26a7fd](https://github.com/hyperi-io/dfe-receiver/commit/c26a7fd4a5a16585e6bf5a8c39b93872fc523640))
* **pgo:** split bind_address from u16 ports in flow section of PGO workload config ([c6ca6f2](https://github.com/hyperi-io/dfe-receiver/commit/c6ca6f23ecc1eaf06e7c41848b62302a6513f5ce))
* prevent double MetricsManager init panic, restructure tests ([fc73caf](https://github.com/hyperi-io/dfe-receiver/commit/fc73cafc4bbbfa50ff0eb9573adc694980c0fa32)), closes [#19](https://github.com/hyperi-io/dfe-receiver/issues/19)
* re-trigger semantic-release for process_batch changes ([b832d7b](https://github.com/hyperi-io/dfe-receiver/commit/b832d7b26eac2cbf60cc39fc38eb8e7e10236300))
* **release:** force patch bump v1.15.10 ([dacc55e](https://github.com/hyperi-io/dfe-receiver/commit/dacc55e287d36ad1db7850ba8152eef470ca29c6))
* **release:** force patch bump v1.15.11 ([778b483](https://github.com/hyperi-io/dfe-receiver/commit/778b4839dd9a1d5b2b44c7c4cc94962e9ca03fe6))
* remove [patch.crates-io], bump rustlib to >=1.16.3 (published) ([86b2b7d](https://github.com/hyperi-io/dfe-receiver/commit/86b2b7d26f926803b75ef9cdccbf3708b75c145d))
* remove duplicate schema_version and oci_labels fields ([8eacd53](https://github.com/hyperi-io/dfe-receiver/commit/8eacd53e8dba86514d513a661df79c9860ba152b))
* remove MSRV pin and fix tautological test assertion ([89d09ee](https://github.com/hyperi-io/dfe-receiver/commit/89d09ee43290afd26b8b66b604e0184dc53a2606))
* remove orphan ci submodule reference — breaks checkout on CI ([b84c09e](https://github.com/hyperi-io/dfe-receiver/commit/b84c09e267eaed367c8b5a2525be6bc3a296d555))
* remove plugin system, document sidecar transport pattern ([6fd8ea4](https://github.com/hyperi-io/dfe-receiver/commit/6fd8ea464845738e1f5576fd4524e138110ebf05))
* remove tracked target symlink — breaks CI runners ([582e627](https://github.com/hyperi-io/dfe-receiver/commit/582e6273c3287f7b67fc38d083724762efb65ec8))
* remove unknown cross build strategy from config ([603a8a8](https://github.com/hyperi-io/dfe-receiver/commit/603a8a881e6f4dcd52eec5e3ec035682265e51ba))
* replace hard-coded DLQ routing with unified rustlib dlq module ([f75f252](https://github.com/hyperi-io/dfe-receiver/commit/f75f2520b2b2be17d4af943f56eb9832724bfbc8))
* replace invalid Renovate preset :pinActionsToFullSha with helpers:pinGitHubActionDigestsToSemver ([fb3befa](https://github.com/hyperi-io/dfe-receiver/commit/fb3befa7e274e59a7b2e750be1fba19028e9f21a))
* resolve all cargo clippy and fmt errors for hyperi-ci pipeline ([70c27a8](https://github.com/hyperi-io/dfe-receiver/commit/70c27a803f3c33365b5ac2999ab8dbdc427f117c))
* resolve allocator conflict when both jemalloc and mimalloc features enabled ([697b55f](https://github.com/hyperi-io/dfe-receiver/commit/697b55f7ccf5eb08252ea3fb2c577d4591003493))
* resolve clippy approx_constant and cargo fmt issues ([3bc6ae1](https://github.com/hyperi-io/dfe-receiver/commit/3bc6ae19fc96404b9c825be7e19cc4057579fceb))
* resolve clippy errors in lumberjack codec and splunk hec handler ([f38696a](https://github.com/hyperi-io/dfe-receiver/commit/f38696a684341a244c2d566916e5abe9f4355452))
* resolve gitleaks false positive on README example token ([87b05eb](https://github.com/hyperi-io/dfe-receiver/commit/87b05ebf63333b35bb097355d12011fca4ec9a35))
* resolve Rust 2024 collapsible_if and feature flag errors ([1a3a28c](https://github.com/hyperi-io/dfe-receiver/commit/1a3a28cf534c5960a70805100c0fd81c4cb4ac1f))
* retrigger CI after runner reset ([9426d63](https://github.com/hyperi-io/dfe-receiver/commit/9426d634601115c2a80a082036f3adb75a52346a))
* retrigger Tier 2 canary on hyperi-ci v1.9.4 channel resolver ([7321d1f](https://github.com/hyperi-io/dfe-receiver/commit/7321d1f5e010b73dba3c268aea1dc476ed2d0cf9))
* retry grpc_sink test sends on transient backpressure ([c53f5f5](https://github.com/hyperi-io/dfe-receiver/commit/c53f5f57bedf8e7b2b11e5c53e4e26520e5578c2))
* scaffold FlowMetrics trait surface + mock impls for tests ([4b74ed7](https://github.com/hyperi-io/dfe-receiver/commit/4b74ed7989086c683301e22fac47c90f4d476670))
* schedule periodic rate-limiter LRU eviction (review [#1](https://github.com/hyperi-io/dfe-receiver/issues/1)) ([cf44a94](https://github.com/hyperi-io/dfe-receiver/commit/cf44a94735afe9f4ee048e95232c6a3ca2327f9b))
* security hardening and dependency updates ([789a4d3](https://github.com/hyperi-io/dfe-receiver/commit/789a4d3444755a55418b58d47f26a3f4e8ca7848))
* skip empty-record envelope emission for flow template packets ([67ba33b](https://github.com/hyperi-io/dfe-receiver/commit/67ba33b6b18ebef1a702b609b08d8e7db78db171))
* stabilise grpc tls test, enable r2 publishing ([df7c8ad](https://github.com/hyperi-io/dfe-receiver/commit/df7c8ad2caf1975184f00fbd97cb407192a72833))
* suppress dead_code warnings on shared test helpers ([f501bf4](https://github.com/hyperi-io/dfe-receiver/commit/f501bf4626c07839e668ac0467a2304b690373c1))
* suppress remaining pedantic clippy lints for hyperi-ci pipeline ([6f9728c](https://github.com/hyperi-io/dfe-receiver/commit/6f9728c5befe3c9a4f8119015310f03e432f2e95))
* switch hyperi-rustlib to crates.io, add base_image to DeploymentContract [skip ci] ([51d4d13](https://github.com/hyperi-io/dfe-receiver/commit/51d4d1369c4cc44a764384fcee3bffe7594a18d7))
* **test:** include binary name in mock --help so contract test asserts pass ([39b58f3](https://github.com/hyperi-io/dfe-receiver/commit/39b58f373ca9ba9905290cb2a0d27cb8489acd30))
* **tests:** grpc_sink large-payload retry + start_server port poll ([fa16c7a](https://github.com/hyperi-io/dfe-receiver/commit/fa16c7a9a38c8e196de78a22e5fff388645ba596))
* **tests:** poll port readiness in protocol→kafka integration tests ([e8bbdf6](https://github.com/hyperi-io/dfe-receiver/commit/e8bbdf63931244a50baad5d8fe88bc2a3d35fb34))
* **test:** write mock binary fallback in contract_artefacts e2e test ([6657048](https://github.com/hyperi-io/dfe-receiver/commit/66570484f47ef2d766371d7cb56196652252c966)), closes [hi#fidelity](https://github.com/hi/issues/fidelity)
* Tier 2 canary — BOLT fix via hyperi-ci v1.10.2 ([910a1c7](https://github.com/hyperi-io/dfe-receiver/commit/910a1c7f5c4924b8f263ab9aac0da739a6c054ad))
* Tier 2 canary on hyperi-ci v1.10.0 (universal tool install) ([366eb89](https://github.com/hyperi-io/dfe-receiver/commit/366eb89134be4cd9e73b39e34e5501d4f2d5656e))
* Tier 2 canary on hyperi-ci v1.10.1 (extended workload grace) ([ee557f1](https://github.com/hyperi-io/dfe-receiver/commit/ee557f1c0a1e78317d15650d4d3382966a05c67e))
* Tier 2 canary on hyperi-ci v1.9.6 workload-arg contract ([9147202](https://github.com/hyperi-io/dfe-receiver/commit/9147202adb6d5d947327526c5c5fedce704b6618))
* Tier 2 canary retrigger on hyperi-ci v1.9.5 ([b84471c](https://github.com/hyperi-io/dfe-receiver/commit/b84471ceb8e91a5000a7d4115141ce12f52281e5))
* trigger CI release after dep upgrades and crates.io migration ([2c6fdd8](https://github.com/hyperi-io/dfe-receiver/commit/2c6fdd851fe905a61be4775c6f2d5e3b57326abd))
* trigger release for metrics migration ([02f7985](https://github.com/hyperi-io/dfe-receiver/commit/02f7985a0470eb7d025ae027a74f40e0febefd43))
* update benchmark to use sonic_rs::from_slice API ([a495f31](https://github.com/hyperi-io/dfe-receiver/commit/a495f313fd32f7467bfda158ddbf1509a475ebab))
* update ci submodule for Cargo.toml lint support ([52eb796](https://github.com/hyperi-io/dfe-receiver/commit/52eb7962a1c8c8fd1bde969898479e7b7af7455d))
* update ci submodule to latest ([769b51b](https://github.com/hyperi-io/dfe-receiver/commit/769b51bdeb322f965f6c848ab9538f793a5015d3))
* update ci submodule with aarch64 cross-compile linker fix ([6602997](https://github.com/hyperi-io/dfe-receiver/commit/66029976d09d5e879ab07249789e2d4f1d14c50e))
* update ci submodule with helm_package_chart stdout fix [skip ci] ([dcba7ab](https://github.com/hyperi-io/dfe-receiver/commit/dcba7ab64445cd48ef36ad22fb032f6b3726e686))
* update ci submodule with libcurl fix ([9855352](https://github.com/hyperi-io/dfe-receiver/commit/98553520b602f9469976743eed0840901849818b))
* update ci submodule with publish-binary source order fix ([7ef80f3](https://github.com/hyperi-io/dfe-receiver/commit/7ef80f382a6b7a08a233f6f520d7f7824c33b5f5))
* update ci submodule with test parallelism fix [skip ci] ([abfd22a](https://github.com/hyperi-io/dfe-receiver/commit/abfd22aa427be3b336fd3c898e6d4caadab91c4a))
* update DfeMetrics::register() to pass &MetricsManager for manifest ([9e051cd](https://github.com/hyperi-io/dfe-receiver/commit/9e051cdd6b31d18f715fe14d1d1ddc3be3cb51ff))
* update Dockerfile header and fix UID 1000 conflict [skip ci] ([2d721fe](https://github.com/hyperi-io/dfe-receiver/commit/2d721fe7b5ad37394944d74eea37245e77192006))
* update docs for plugin removal and CI migration ([ab75d80](https://github.com/hyperi-io/dfe-receiver/commit/ab75d803028d0951cddc97e2c3a8b06644e171a3))
* update hyperi-rustlib 1.20.0 to 1.20.1 ([6b16ecf](https://github.com/hyperi-io/dfe-receiver/commit/6b16ecfe9773e8d38316ee7643e862ef875ed586))
* update KEDA PromQL to dfe_scaling_pressure ([9eac1ae](https://github.com/hyperi-io/dfe-receiver/commit/9eac1aea2bd1eaa1baed2c007e319b0e2135b3bf))
* update rustlib to v1.8.1, fix RwLock access ([1c264c7](https://github.com/hyperi-io/dfe-receiver/commit/1c264c77212553ddec46211b69df8650b6f3f5af))
* update rustls-webpki 0.103.9 → 0.103.10 (GHSA-pwjx-qhcg-rvj4) ([a175f8e](https://github.com/hyperi-io/dfe-receiver/commit/a175f8e4f2de0c02eb01c2e7031bfc9103289d42))
* Update the version constraint ([4677e9b](https://github.com/hyperi-io/dfe-receiver/commit/4677e9bed64eb31068349e25262742b2e2411d9c))
* update to rustlib v2.x ServiceRuntime + releaserc breaking rule ([5fac7ed](https://github.com/hyperi-io/dfe-receiver/commit/5fac7ed0cefde6cabadee5761a81bb9f4574afac))
* upgrade deps to latest, edition 2024, migrate Kafka sinks to rustlib [skip ci] ([8389f40](https://github.com/hyperi-io/dfe-receiver/commit/8389f40e6baf96c2311d4710c73617788c1432f9))
* use GITHUB_TOKEN for GHCR container publish [skip ci] ([303acdc](https://github.com/hyperi-io/dfe-receiver/commit/303acdc443a77034e27acf2c7ba4c4a2a4cdd56f))
* use MemoryGuardConfig::from_env for standard env var overrides ([554036b](https://github.com/hyperi-io/dfe-receiver/commit/554036b2653bfcf4fa351c5ce1fa7586132cb592))
* Use new version of plugin loader and no need for deps dir ([61f4bcf](https://github.com/hyperi-io/dfe-receiver/commit/61f4bcfe1e817563debbd8aebf0e2c6440b67681))
* use non-approx-constant float in test to satisfy clippy ([4dc4297](https://github.com/hyperi-io/dfe-receiver/commit/4dc4297ac634e079f3570058a56ecd61888749ff))
* use ServiceRuntime metrics manager to avoid double recorder panic ([bb32ee4](https://github.com/hyperi-io/dfe-receiver/commit/bb32ee4e9d51b628cfbc44b42d9641770096f67b))
* use standard runner for release workflow instead of buildjet ([9bdd883](https://github.com/hyperi-io/dfe-receiver/commit/9bdd883ec883eee4bb66f6bfe0b799089148baf4))
* use uid 10001 for appuser to avoid collision with ubuntu user in base image ([cce2aa2](https://github.com/hyperi-io/dfe-receiver/commit/cce2aa25fcdae611bfa41b04a7078e56cffb12fb))
* Using symlinks for dependency repos ([07c62dd](https://github.com/hyperi-io/dfe-receiver/commit/07c62ddccbbf868ecdaf3045d4ff570b9f2bdde4))
* vendor google/protobuf/timestamp.proto for CI protoc compatibility ([6e6f5f0](https://github.com/hyperi-io/dfe-receiver/commit/6e6f5f0959406ac71066e0dbbad0c009c232f8ad))
* wire FlowHandler into server orchestration + flat-env + config.example ([c21bb5f](https://github.com/hyperi-io/dfe-receiver/commit/c21bb5f13b4c39ce2699720a0ed72e43dae2ae8a))
* wire log spam helpers into identified hot spots ([fc1b9a5](https://github.com/hyperi-io/dfe-receiver/commit/fc1b9a50c62b271a56f070adaf9389f440389990))
* wire rustlib cli/deployment module and generate artefacts ([c37d3b0](https://github.com/hyperi-io/dfe-receiver/commit/c37d3b08c43cfc6350fa81e9bbf28d09f4399fa1))
* wire security event logging into auth, TLS, and config reload ([47ec9f7](https://github.com/hyperi-io/dfe-receiver/commit/47ec9f769f417dd7ed6a4033d795c05168f94565))
* wire up KEDA scaling metric with gated composite logic ([d6e7333](https://github.com/hyperi-io/dfe-receiver/commit/d6e73339a22fc6a1b20d00705c3a4a2b3530424a))


### Features

* add dynamic plugin system with C ABI loader ([73ec348](https://github.com/hyperi-io/dfe-receiver/commit/73ec348daf634bf6c31a024807955cc2a9b1841b))
* add gRPC loader transport and file debug sink ([69b3110](https://github.com/hyperi-io/dfe-receiver/commit/69b3110158dc935a2814bdea8780af000672a98d))
* add grpc vector protocol, tls hot-reload, bearer auth tests and rebrand fixes ([775c492](https://github.com/hyperi-io/dfe-receiver/commit/775c492e7b7ef8e9972d284d7cc0fb890be0a018))
* add Lumberjack v2 (Beats) protocol handler with Filebeat integration tests ([adbd33f](https://github.com/hyperi-io/dfe-receiver/commit/adbd33faa6d75821a2ec70aebe9e5655d8124cda))
* add opt-in disk spillover via rustlib TieredSink ([dd3a674](https://github.com/hyperi-io/dfe-receiver/commit/dd3a674c1a96bf5231974ee7f5da0440b2167128))
* add optional Prometheus scaling trigger to KEDA ScaledObject ([3696316](https://github.com/hyperi-io/dfe-receiver/commit/36963162c26085a6fa1d335c42ce65fe9a0cdfa2))
* add OTLP protocol support with dual-mode conversion ([8e3eac2](https://github.com/hyperi-io/dfe-receiver/commit/8e3eac26e023de309e848faaff103912a1e9f30e))
* add PGO workload driver + performance docs ([2c580e9](https://github.com/hyperi-io/dfe-receiver/commit/2c580e95a60ebfb1e99f6764fce0a5ff9153ec23))
* add prometheus remote write v1 receiver ([bacca52](https://github.com/hyperi-io/dfe-receiver/commit/bacca524d80c48a360f014669794324356d371bb))
* add RustlibSinkAdapter for bridging sink traits ([746555f](https://github.com/hyperi-io/dfe-receiver/commit/746555f1aba0badd9292c809276d15b7a13db3dc))
* add Splunk HEC protocol handler with integration tests ([d56d077](https://github.com/hyperi-io/dfe-receiver/commit/d56d0772f8c51d4b8a4d9f21c0fa76b72529ed3f))
* add syslog protocol handler (UDP + TCP + TLS) ([82c49a1](https://github.com/hyperi-io/dfe-receiver/commit/82c49a13dbdb6b7794c9ab6b88a539c06c110c2b))
* **auth:** add bearer token authentication with secret manager support ([f1692fb](https://github.com/hyperi-io/dfe-receiver/commit/f1692fbc67a2a4cf9b36fb01ebe61906e944d45a))
* configure rust feature sets for CI testing ([7252a70](https://github.com/hyperi-io/dfe-receiver/commit/7252a70e3337487f2311036ae4dd6792280d8dfb))
* **deps:** switch hs-rustlib from git to JFrog Cargo registry ([b268ff3](https://github.com/hyperi-io/dfe-receiver/commit/b268ff3387a16363df84d7e00c50b2e4777c4f28))
* enable container and Helm publishing with multi-arch Dockerfile ([226f90c](https://github.com/hyperi-io/dfe-receiver/commit/226f90cba3a763e2fe5ca26f491b10a30d15a88a))
* initial dfe-receiver implementation ([14cc2c0](https://github.com/hyperi-io/dfe-receiver/commit/14cc2c05e0ccefb4af55bc2b8805a24743a9e2f1)), closes [Hi#performance](https://github.com/Hi/issues/performance)
* **security:** add HTTP security hardening and Rust CI ([f05b8f4](https://github.com/hyperi-io/dfe-receiver/commit/f05b8f4a45904ff581979fc1e619d2a15b9f6293))
* SharedConfig, env overrides, config reload, serde_yaml_ng migration ([60f838c](https://github.com/hyperi-io/dfe-receiver/commit/60f838cf012f2ceea94289e55f34a270d7d3e96d))
* source-rule routing, timestamp enrichment, config refresh ([c9aefd6](https://github.com/hyperi-io/dfe-receiver/commit/c9aefd63117802d3310e5123198545f7e9f3fc01))
* TLS from secrets + remove disk spillover ([c7671c0](https://github.com/hyperi-io/dfe-receiver/commit/c7671c03ae8e8bb38a5d3ecd254a87781593ab09))
* wire SharedConfig hot-reload to auth state ([fa85a72](https://github.com/hyperi-io/dfe-receiver/commit/fa85a72b0d3ccea8d416a86877e1eeea6771189a))


### Performance Improvements

* expand benchmark suite with router and metrics render groups ([87ba7e7](https://github.com/hyperi-io/dfe-receiver/commit/87ba7e73c817e9146655efc0a637eeb4852f288a))

## [1.15.10](https://github.com/hyperi-io/dfe-receiver/compare/v1.15.9...v1.15.10) (2026-05-06)


### Bug Fixes

* **cli:** align with dfe-loader StandardCommand pattern ([0b06511](https://github.com/hyperi-io/dfe-receiver/commit/0b06511c6e89c5b9b80d425eb6bf339e0d304ee5))
* **release:** force patch bump v1.15.10 ([0979e8a](https://github.com/hyperi-io/dfe-receiver/commit/0979e8aabaf6f3a47e2840f8bc2d3c3e6b343e25))

## [1.15.9](https://github.com/hyperi-io/dfe-receiver/compare/v1.15.8...v1.15.9) (2026-05-02)


### Bug Fixes

* **deployment:** wire DfeApp::deployment_contract trait hook + bump rustlib to >=2.7.0 ([70aa9c3](https://github.com/hyperi-io/dfe-receiver/commit/70aa9c3435b9b27b7f117fb3bbf5d46c40e915a3))
* **deps:** track rustlib 2.6.1 (cli→cli-service, worker→worker-pool) ([7160fbc](https://github.com/hyperi-io/dfe-receiver/commit/7160fbc3780b4f4bc6f240cb31bbb950873b1d33))

## [1.15.8](https://github.com/hyperi-io/dfe-receiver/compare/v1.15.7...v1.15.8) (2026-04-22)


### Bug Fixes

* canary release through BOLT + R2 on ARC runner v1.12.1 ([06858ac](https://github.com/hyperi-io/dfe-receiver/commit/06858acf84ea1254e658e49c6829cb986f9931cd))
* **tests:** grpc_sink large-payload retry + start_server port poll ([030ce51](https://github.com/hyperi-io/dfe-receiver/commit/030ce5146141aae825a23a09b28b986562d0a1f1))
* **tests:** poll port readiness in protocol→kafka integration tests ([f80d851](https://github.com/hyperi-io/dfe-receiver/commit/f80d8519626b9f88593f1f5c799793fa0929d780))
* Tier 2 canary — BOLT fix via hyperi-ci v1.10.2 ([3bafba7](https://github.com/hyperi-io/dfe-receiver/commit/3bafba72bdf40a5ed544d9ddd82ec9a0ef43e8a1))

## [1.15.7](https://github.com/hyperi-io/dfe-receiver/compare/v1.15.6...v1.15.7) (2026-04-19)


### Bug Fixes

* retry grpc_sink test sends on transient backpressure ([1e44441](https://github.com/hyperi-io/dfe-receiver/commit/1e4444149356e098deaea0dc7766c34095daf071))

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

* drop stray submodule pin

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
