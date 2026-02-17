# CI Requirements: dfe-receiver Publishing

## Issue

The `dfe-receiver` project cannot publish to JFrog Cargo registry because it depends on `hyperi-rustlib` via a git dependency:

```toml
hyperi-rustlib = { git = "https://github.com/hyperi-io/hyperi-rustlib", features = [...] }
```

Cargo requires all dependencies to have version requirements when publishing to a registry.

## Current Error

```
error: failed to verify manifest at `/home/runner/.../Cargo.toml`

Caused by:
  all dependencies must have a version requirement specified when publishing.
  dependency `hyperi-rustlib` does not specify a version
```

## Solution Options

### Option 1: Registry Dependency (Recommended)

Update `Cargo.toml` to use the JFrog Cargo registry version:

```toml
hyperi-rustlib = { version = "1.3", registry = "hyperi", features = [...] }
```

**Requirements:**

- hyperi-rustlib must be published to JFrog Cargo registry first
- Projects must configure the hyperi registry in `.cargo/config.toml`

### Option 2: Dual Dependencies with Cargo Feature

Use a cargo feature to switch between git (development) and registry (publish):

```toml
[dependencies]
# Registry version for publishing
hyperi-rustlib = { version = "1.3", registry = "hyperi", features = [...], optional = true }

[features]
default = ["hyperi-rustlib"]
git-deps = []  # Use this for local development

[target.'cfg(feature = "git-deps")'.dependencies]
hyperi-rustlib = { git = "https://github.com/hyperi-io/hyperi-rustlib", features = [...] }
```

### Option 3: cargo-patch in CI

Use `cargo-patch` or similar tool in CI to replace git dependencies with registry versions before publishing.

## Recommended Approach

For projects that depend on `hyperi-rustlib`:

1. **Development:** Keep using git dependency for access to latest changes
2. **Release:** CI should update Cargo.toml to use registry version before `cargo publish`

The CI workflow should:

1. Parse the git dependency to find the current commit/ref
2. Map to the published version in JFrog Cargo
3. Update Cargo.toml temporarily for publishing
4. Run `cargo publish --registry hyperi`

## Workflow Update Needed

Add a step to the release workflow that patches the Cargo.toml:

```yaml
- name: Patch git deps for publishing
  run: |
    # Replace git dependency with registry version
    sed -i 's|hyperi-rustlib = { git = "https://github.com/hyperi-io/hyperi-rustlib"|hyperi-rustlib = { version = "1.3", registry = "hyperi"|g' Cargo.toml
    cat Cargo.toml
```

Or use a more sophisticated approach with `toml-cli` or custom script.

## Alternative: Binary-only Publishing

If source distribution to JFrog Cargo is not required, the release workflow could:

1. Build the binary
2. Upload the binary to JFrog Generic repo
3. Skip `cargo publish`

This avoids the dependency version issue entirely.
