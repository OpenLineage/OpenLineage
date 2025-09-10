#!/usr/bin/env bash
#
# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Build script for OpenLineage SQL parser.
# It's assumed that it will be run on MacOS
set -e

if [[ -d "./iface-py" ]]
then
  cd iface-py
fi

echo "Checking for Rust"
if [[ ! $(command -v cargo) ]]
then
  echo "Installing Rust"
  curl https://sh.rustup.rs -sSf | sh -s -- -y
  source "$HOME"/.cargo/env
fi

echo "Checking for uv"
if [[ ! $(command -v uv) ]]
then
  echo "Installing uv"
  curl -LsSf https://astral.sh/uv/install.sh | sh
  source "$HOME"/.local/bin/env
fi

rustup target add aarch64-apple-darwin
rustup target add x86_64-apple-darwin

echo "Installing Python 3.10"
uv python install 3.10

# Maturin is build tool that we're using. It can build python wheels based on standard Rust Cargo.toml.
echo "Installing Maturin"
uv sync --no-install-project

# Disable incremental compilation, since it causes issues.
export CARGO_INCREMENTAL=0

# Run test if indicated to do so.
if [[ $RUN_TESTS = true ]]; then
  cargo test --no-default-features
  cargo clippy --all-targets --all-features -- -D warnings
  cargo fmt -- --check
fi

# Build release wheels
uv tool run maturin build --target universal2-apple-darwin --out target/wheels --release --strip

echo "Package build, trying to import"
echo "Platform:"
echo "import platform; print(platform.platform())" | uv run -
# Verify that it imports and works properly
uv pip install openlineage-sql --no-index --find-links target/wheels --force-reinstall
echo "from openlineage_sql import parse, ColumnLineage; import sys; sys.exit(len(parse([\"SELECT b.a from b\"]).column_lineage) != 1)" | uv run -
echo "all good"