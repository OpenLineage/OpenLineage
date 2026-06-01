#!/bin/sh
# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
set -e

if ls /opt/wheels/*.whl >/dev/null 2>&1; then
  echo "Installing OpenLineage wheels from /opt/wheels ..."
  uv pip install --system /opt/wheels/*.whl
else
  echo "No wheels in /opt/wheels — run scripts/build-wheels.sh before integration tests."
fi

exec "$@"
