#!/usr/bin/env bash
# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
WHEELS_DIR="${TEST_DIR}/wheels"
OPENLINEAGE_ROOT="$(cd "${TEST_DIR}/../../../.." && pwd)"

mkdir -p "${WHEELS_DIR}"
rm -f "${WHEELS_DIR}"/*.whl

echo "Building client/python wheel..."
uv build --wheel --out-dir "${WHEELS_DIR}" "${OPENLINEAGE_ROOT}/client/python"

DBT_VERSION="$(
  python3 -c "import tomllib; print(tomllib.load(open('${OPENLINEAGE_ROOT}/integration/dbt/pyproject.toml','rb'))['project']['version'])"
)"
SQL_DIST="${OPENLINEAGE_ROOT}/integration/sql/iface-py/dist"
SQL_WHEEL="$(find "${SQL_DIST}" -name "openlineage_sql-${DBT_VERSION}-*linux*.whl" 2>/dev/null | head -1 || true)"

if [[ -n "${SQL_WHEEL}" ]]; then
  echo "Using existing SQL wheel: ${SQL_WHEEL}"
  cp "${SQL_WHEEL}" "${WHEELS_DIR}/"
else
  echo "Building integration/sql wheel via Docker..."
  docker build -t openlineage-sql-builder \
    -f "${OPENLINEAGE_ROOT}/integration/sql/Dockerfile.python" \
    "${OPENLINEAGE_ROOT}/integration/sql"
  docker run --rm -v "${WHEELS_DIR}:/output" openlineage-sql-builder \
    sh -c "cp /wheels/* /output/"
fi

echo "Building integration/common wheel..."
uv build --wheel --out-dir "${WHEELS_DIR}" "${OPENLINEAGE_ROOT}/integration/common"

echo "Building integration/dbt wheel..."
uv build --wheel --out-dir "${WHEELS_DIR}" "${OPENLINEAGE_ROOT}/integration/dbt"

echo "Wheels ready in ${WHEELS_DIR}:"
ls -la "${WHEELS_DIR}"/*.whl
