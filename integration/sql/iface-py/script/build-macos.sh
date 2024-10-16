#!/usr/bin/env bash
#
# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Build script for OpenLineage SQL parser.
# It's assumed that it will be run on MacOS
set -e

# Disable incremental compilation, since it causes issues.
export CARGO_INCREMENTAL=0

# Run test if indicated to do so.
if [[ $RUN_TESTS = true ]]; then
  cargo test --no-default-features
  cargo clippy --all-targets --all-features -- -D warnings
  cargo fmt -- --check
fi

# Build release wheels
if [[ -d "./iface-py" ]]
then
  cd iface-py
fi
uv tool run maturin build --target universal2-apple-darwin --out target/wheels --release --strip

echo "Package build, trying to import"
echo "Platform:"
python -c "from distutils import util; print(util.get_platform())"
# Verify that it imports and works properly
python -m pip install openlineage-sql --no-index --find-links target/wheels --force-reinstall
python -c "from openlineage_sql import parse, ColumnLineage; import sys; sys.exit(len(parse([\"SELECT b.a from b\"]).column_lineage) != 1)"
echo "all good"