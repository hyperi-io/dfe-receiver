# TODO - dfe-receiver

This is the **single source of truth** for all tasks and progress.

---

## Active Tasks

Tasks currently being worked on. Only one task should be `[IN PROGRESS]` at a time.

_No active tasks_

---

## Work Breakdown Structure (WBS)

When planning complex features, break them down here before starting.

_No features in planning_

---

## Completed (This Session)

Move tasks here when done. Clear this section at end of session.

- [x] Use hyperi-rustlib CircuitBreaker in TieredSink (removed redundant implementation)
- [x] Add `secrets` feature to hyperi-rustlib dependency
- [x] Create GitHub repo at hyperi-io/dfe-receiver and push initial commit
- [x] Add bearer token authentication support
  - [x] Add BearerConfig to AuthConfig
  - [x] Create BearerTokenProvider with secret manager integration
  - [x] Add validate_bearer_auth() for Authorization header
  - [x] Update auth middleware for bearer mode
  - [x] Add From<SecretsError> conversion
  - [x] Add comprehensive tests

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
