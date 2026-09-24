#!/usr/bin/env bash
# Project:   dfe-receiver
# File:      scripts/fetch-fluent-bit.sh
# Purpose:   Download and cache Fluent Bit binary for integration tests
# Language:  Bash
#
# License:   BUSL-1.1
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# Usage:
#   ./scripts/fetch-fluent-bit.sh              # ensure latest, print binary path
#   FLUENT_BIT_VERSION=4.2.3 ./scripts/fetch-fluent-bit.sh  # pin specific version
#
# Downloads the latest Fluent Bit release from packages.fluentbit.io only if
# the cached binary is missing or out of date. Prints the absolute path to the
# fluent-bit binary on stdout (last line). Status messages go to stderr.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CACHE_DIR="${REPO_ROOT}/.tmp/fluent-bit"
ARCH="$(uname -m)"

die() { echo "ERROR: ${1}" >&2; exit "${2:-1}"; }

# Map uname -m to Debian arch names
deb_arch() {
    case "${ARCH}" in
        x86_64)  echo "amd64" ;;
        aarch64) echo "arm64" ;;
        *)       die "unsupported architecture: ${ARCH}" ;;
    esac
}

# Read version from cached binary
cached_version() {
    local bin="${CACHE_DIR}/bin/fluent-bit"
    if [[ -x "${bin}" ]]; then
        "${bin}" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1
    fi
}

# Resolve the latest stable version from GitHub releases
resolve_latest_version() {
    if command -v gh > /dev/null 2>&1; then
        gh release list --repo fluent/fluent-bit --limit 30 --json tagName \
            --jq '[.[] | select(.tagName | test("^v[0-9]+\\.[0-9]+\\.[0-9]+$"))][0].tagName' \
            | sed 's/^v//'
    elif command -v jq > /dev/null 2>&1; then
        curl -fsSL "https://api.github.com/repos/fluent/fluent-bit/releases?per_page=30" \
            | jq -r '[.[] | select(.tag_name | test("^v[0-9]+\\.[0-9]+\\.[0-9]+$"))][0].tag_name' \
            | sed 's/^v//'
    else
        die "need either 'gh' or 'jq' to resolve latest version"
    fi
}

# Resolve the desired version
if [[ -n "${FLUENT_BIT_VERSION:-}" ]]; then
    WANT_VERSION="${FLUENT_BIT_VERSION}"
else
    WANT_VERSION="$(resolve_latest_version)"
fi

if [[ -z "${WANT_VERSION}" || "${WANT_VERSION}" == "null" ]]; then
    die "could not resolve Fluent Bit version"
fi

BINARY="${CACHE_DIR}/bin/fluent-bit"
HAVE_VERSION="$(cached_version || true)"

# If cached binary matches desired version, use it
if [[ "${HAVE_VERSION}" == "${WANT_VERSION}" ]]; then
    echo "Fluent Bit ${WANT_VERSION} already cached" >&2
    echo "${BINARY}"
    exit 0
fi

if [[ -n "${HAVE_VERSION}" ]]; then
    echo "Updating Fluent Bit ${HAVE_VERSION} -> ${WANT_VERSION}" >&2
else
    echo "Downloading Fluent Bit ${WANT_VERSION} for ${ARCH}..." >&2
fi

# Clean old cache
rm -rf "${CACHE_DIR:?}/bin"

# Fluent Bit distributes via .deb packages on packages.fluentbit.io.
# Use noble (24.04 LTS) as the base codename -- binary is portable across
# Ubuntu versions since it bundles its own libs under /opt/fluent-bit/.
DEB_CODENAME="noble"
DEB_ARCH="$(deb_arch)"
DEB_URL="https://packages.fluentbit.io/ubuntu/${DEB_CODENAME}/pool/main/f/fluent-bit/fluent-bit_${WANT_VERSION}_${DEB_ARCH}.deb"

mkdir -p "${CACHE_DIR}"
DEB_FILE="${CACHE_DIR}/fluent-bit.deb"

echo "Downloading from ${DEB_URL}" >&2
curl -fSL --progress-bar -o "${DEB_FILE}" "${DEB_URL}"

# Extract binary from .deb (no root needed)
echo "Extracting..." >&2
EXTRACT_DIR="${CACHE_DIR}/extracted"
mkdir -p "${EXTRACT_DIR}"
dpkg-deb -x "${DEB_FILE}" "${EXTRACT_DIR}"

# Move binary and libs to cache
mkdir -p "${CACHE_DIR}/bin"
cp "${EXTRACT_DIR}/opt/fluent-bit/bin/fluent-bit" "${CACHE_DIR}/bin/fluent-bit"
chmod +x "${CACHE_DIR}/bin/fluent-bit"

# Copy shared libs if present (some builds bundle them)
if [[ -d "${EXTRACT_DIR}/opt/fluent-bit/lib" ]]; then
    cp -r "${EXTRACT_DIR}/opt/fluent-bit/lib" "${CACHE_DIR}/lib"
fi

# Cleanup
rm -rf "${EXTRACT_DIR}" "${DEB_FILE}"

# Verify
if [[ ! -x "${BINARY}" ]]; then
    die "Fluent Bit binary not found at ${BINARY} after extraction"
fi

local_version="$("${BINARY}" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)"
echo "Fluent Bit ${local_version} cached at ${BINARY}" >&2
echo "${BINARY}"
