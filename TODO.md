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
- [x] Rebrand hyperi-rustlib to hyperi-rustlib across codebase
- [x] Rebrand x-hypersec-agent to x-hyperi-agent, rename CI/reference configs
- [x] Fix all license headers to FSL-1.1-ALv2
- [x] gRPC Vector sink protocol implementation
  - [x] Rewrite proto/vector.proto to match Vector upstream (unary PushEvents)
  - [x] Create proto/event.proto with Vector event types
  - [x] Implement protobuf-to-JSON conversion (src/server/grpc/convert.rs)
  - [x] Rewrite gRPC handler from streaming to unary
  - [x] Add TLS support and auth interceptor to gRPC server
  - [x] Extend GrpcConfig with tls + auth fields
- [x] TLS/mTLS certificate hot-reload from secret manager
  - [x] Create TlsCertProvider with background refresh task
  - [x] Add build_grpc_tls_config() for tonic TLS
  - [x] Integrate TlsCertProvider into HTTP server accept loop
- [x] Integration tests for file-based bearer auth
  - [x] test_bearer_auth_from_file
  - [x] test_bearer_auth_file_refresh
  - [x] test_bearer_auth_file_comma_separated
- [x] Vector integration tests (local vector cmdline + yaml, HTTPS + gRPC)
  - [x] test_vector_http_sink (plaintext HTTP)
  - [x] test_vector_https_sink (HTTPS with self-signed cert)
  - [x] test_vector_grpc_sink (gRPC Vector protocol)
  - [x] test_vector_grpc_tls_sink (gRPC with TLS)
  - [x] test_vector_http_bearer_auth (HTTP with bearer token)
  - Gracefully skips on machines where vector is not installed

---

## Backlog

Future work, ordered by priority.

### High Priority

- [ ] KEDA scaling metrics endpoint

### Medium Priority

- [ ] Config hot-reload for auth settings
- [ ] Performance benchmarks

### Low Priority

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
