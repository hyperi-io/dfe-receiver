#!/usr/bin/env bash
# Project:   dfe-receiver
# File:      scripts/fetch-filebeat.sh
# Purpose:   Download and cache Filebeat binary for integration tests
# Language:  Bash
#
# License:   BUSL-1.1
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# Usage:
#   ./scripts/fetch-filebeat.sh                        # ensure latest, print binary path
#   FILEBEAT_VERSION=9.3.1 ./scripts/fetch-filebeat.sh # pin specific version
#
# Downloads the latest Filebeat release only if the cached binary is missing or
# out of date. Prints the absolute path to the filebeat binary on stdout (last line).
# Status messages go to stderr.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CACHE_DIR="${REPO_ROOT}/.tmp/filebeat"
ARCH="$(uname -m)"

# Map arch to Elastic naming convention
case "$ARCH" in
    x86_64)  ELASTIC_ARCH="x86_64" ;;
    aarch64) ELASTIC_ARCH="arm64" ;;
    *)
        echo "ERROR: unsupported architecture: ${ARCH}" >&2
        exit 1
        ;;
esac

# Check what we have cached (read version from binary)
cached_version() {
    local bin="${CACHE_DIR}/bin/filebeat"
    if [[ -x "$bin" ]]; then
        "$bin" version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1
    fi
}

# Resolve the desired version
if [[ -n "${FILEBEAT_VERSION:-}" ]]; then
    WANT_VERSION="$FILEBEAT_VERSION"
else
    if command -v gh &>/dev/null; then
        WANT_VERSION=$(gh release list --repo elastic/beats --limit 30 --json tagName \
            --jq '[.[] | select(.tagName | test("^v9\\."))][0].tagName' | sed 's/^v//')
    elif command -v jq &>/dev/null; then
        WANT_VERSION=$(curl -fsSL "https://api.github.com/repos/elastic/beats/releases?per_page=30" \
            | jq -r '[.[] | select(.tag_name | test("^v9\\."))][0].tag_name' | sed 's/^v//')
    else
        echo "ERROR: need either 'gh' or 'jq' to resolve latest version" >&2
        exit 1
    fi
fi

if [[ -z "$WANT_VERSION" || "$WANT_VERSION" == "null" ]]; then
    echo "ERROR: could not resolve Filebeat version" >&2
    exit 1
fi

BINARY="${CACHE_DIR}/bin/filebeat"
HAVE_VERSION=$(cached_version || true)

# If cached binary matches desired version, use it
if [[ "$HAVE_VERSION" == "$WANT_VERSION" ]]; then
    echo "Filebeat ${WANT_VERSION} already cached" >&2
    echo "$BINARY"
    exit 0
fi

if [[ -n "$HAVE_VERSION" ]]; then
    echo "Updating Filebeat ${HAVE_VERSION} -> ${WANT_VERSION}" >&2
else
    echo "Downloading Filebeat ${WANT_VERSION} for ${ARCH}..." >&2
fi

# Clean old cache
rm -rf "${CACHE_DIR:?}/bin"

# Download from Elastic artifacts
mkdir -p "${CACHE_DIR}"
TARBALL_NAME="filebeat-${WANT_VERSION}-linux-${ELASTIC_ARCH}.tar.gz"
DOWNLOAD_URL="https://artifacts.elastic.co/downloads/beats/filebeat/${TARBALL_NAME}"

curl -fSL --progress-bar -o "${CACHE_DIR}/${TARBALL_NAME}" "$DOWNLOAD_URL"

# Extract -- tarball contains filebeat-{version}-linux-{arch}/ with filebeat binary at root
echo "Extracting..." >&2
tar xzf "${CACHE_DIR}/${TARBALL_NAME}" -C "${CACHE_DIR}"

EXTRACTED_DIR="${CACHE_DIR}/filebeat-${WANT_VERSION}-linux-${ELASTIC_ARCH}"
if [[ -d "$EXTRACTED_DIR" ]]; then
    mkdir -p "${CACHE_DIR}/bin"
    mv "${EXTRACTED_DIR}/filebeat" "${CACHE_DIR}/bin/filebeat"
    rm -rf "$EXTRACTED_DIR"
fi

# Cleanup tarball
rm -f "${CACHE_DIR}/${TARBALL_NAME}"

# Verify
if [[ ! -x "$BINARY" ]]; then
    echo "ERROR: Filebeat binary not found at ${BINARY} after extraction" >&2
    exit 1
fi

echo "Filebeat ${WANT_VERSION} cached at ${BINARY}" >&2
echo "$BINARY"
