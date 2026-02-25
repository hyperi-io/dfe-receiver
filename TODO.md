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
