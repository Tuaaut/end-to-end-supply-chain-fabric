#!/usr/bin/env bash
set -euo pipefail

# Build an isolated Azure Function deployment copy.
# The local incremental generator under ../src remains untouched.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
FUNCTION_DIR="${ROOT_DIR}/azure_function/event_driven"
APP_DIR="${FUNCTION_DIR}/app"
BUILD_DIR="${FUNCTION_DIR}/build_output"

rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}/src"
mkdir -p "${BUILD_DIR}/generate_batch"
cp "${APP_DIR}/host.json" "${BUILD_DIR}/host.json"
cp "${APP_DIR}/requirements.txt" "${BUILD_DIR}/requirements.txt"
cp "${ROOT_DIR}/src/generate_incremental_batch.py" "${BUILD_DIR}/src/generate_incremental_batch.py"
cp "${APP_DIR}/generate_batch/__init__.py" "${BUILD_DIR}/generate_batch/__init__.py"
cp "${APP_DIR}/generate_batch/function.json" "${BUILD_DIR}/generate_batch/function.json"
cp "${APP_DIR}/event_batch.py" "${BUILD_DIR}/event_batch.py"

printf 'Built isolated Function package at %s\n' "${BUILD_DIR}"
