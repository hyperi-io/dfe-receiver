# TODO - dfe-receiver

This is the **single source of truth** for all tasks and progress.

---

## Active Tasks

Tasks currently being worked on. Only one task should be `[IN PROGRESS]` at a time.

- [ ] Merge `feat/source-routing-enrichment` branch to main (pending review)

---

## Work Breakdown Structure (WBS)

When planning complex features, break them down here before starting.

_No features in planning_

---

## Completed (This Session)

Move tasks here when done. Clear this section at end of session.

- [x] Rebrand hs-rustlib/hypersec → hyperi-rustlib/hyperi (branch `chore/rebrand-hyperi`, merged to main)
- [x] Rename env prefix `RECEIVER_` → `DFE_RECEIVER_` for all config env vars
- [x] Add `include_common_header` bool to AuthConfig (default true), gates enrichment
- [x] Implement rule-based `_source` routing (key_present, key_value_set, key_value_use)
  - [x] New `SourceRule` struct, `RoutingConfig` rewrite
  - [x] `Router` rewrite with `evaluate_source()` method
  - [x] Legacy compat mode for `tags.event.category` / `event_category`
  - [x] Default source "dfe" (was "unmatched"), source-to-topic remapping
- [x] Inject `_timestamp_receiver` (epoch ms) into JSON payload on hot path
- [x] Config hot-reload via SIGHUP (Router/Validator wrapped in RwLock)
- [x] CI runner default changed to `arc-runner-16cpu`
- [x] Updated config.example.yaml, docs/DESIGN.md

---

## Backlog

Future work, ordered by priority.

### High Priority

- [ ] gRPC Vector sink protocol implementation
- [ ] TLS/mTLS certificate loading from secret manager
- [ ] Integration tests for bearer auth with real secret providers

### Medium Priority

- [ ] **Vector.dev embedded receiver module** — [DISCUSSION]
  - Supply a `vector.yaml` config file with a commonly-configured sink targeting the core dfe-receiver JSON processor
  - Artefacts: vector.yaml template/reference config
  - Investigate linking/embedding the Vector binary as a Rust library (not subprocess)
    - Vector is not officially designed as an embeddable library ([Discussion #19776](https://github.com/vectordotdev/vector/discussions/19776))
    - Individual crates (`vector-core`, `vector-lib`, `vrl`) may be usable as git dependencies
    - Extensive feature flags allow selective compilation of only needed components
  - Licensing: Vector is [MPL-2.0](https://github.com/vectordotdev/vector/blob/master/LICENSE), dfe-receiver is FSL-1.1-ALv2
    - MPL-2.0 file-level copyleft allows combining with non-MPL code in a larger work
    - MPL-licensed source files must remain available under MPL-2.0
    - No formal compatibility declaration between MPL-2.0 and FSL-1.1 exists — legal review needed
  - See discussion notes below
- [ ] KEDA scaling metrics endpoint
- [ ] Disk spillover implementation (currently in-memory only)
- [ ] Config hot-reload for auth settings

### Low Priority

- [ ] Performance benchmarks
- [ ] Documentation for deployment

---

## Blocked

_None_

---

## Notes for AI Assistants

This file is the **single source of truth** for tasks and progress.

**Rules:**

- All tasks go here, nowhere else
- Planning mode outputs go here (WBS section)
- Mark tasks `[IN PROGRESS]` when starting
- Mark tasks `[x]` when complete, move to Completed section
- Never add tasks to STATE.md or CLAUDE.md

**Status tags:**

- `[PENDING]` - Not started
- `[IN PROGRESS]` - Currently working on
- `[BLOCKED]` - Waiting on something
- `[x]` - Completed (checkbox checked)
