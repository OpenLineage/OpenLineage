#!/usr/bin/env bash
#
# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Build script for OpenLineage SQL parser.
# It's assumed that it will be run on MacOS
set -e

echo "Installing homebrew"
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

export HOMEBREW_NO_AUTO_UPDATE=1
export HOMEBREW_NO_INSTALL_CLEANUP=1

echo "Installing Python 3.7"
brew install python@3.7

which python
which python3

/usr/local/bin/python3.7 -m venv .env
source .env/bin/activate

which python
which python3

# Install Rust
echo "Installing Rust"
curl https://sh.rustup.rs -sSf | sh -s -- -y
source $HOME/.cargo/env

rustup target add aarch64-apple-darwin

# Maturin is build tool that we're using. It can build python wheels based on standard Rust Cargo.toml.
echo "Installing Maturin"
python -m pip install maturin

# Disable incremental compilation, since it causes issues.
export CARGO_INCREMENTAL=0

# Run test if indicated to do so.
if [[ -z ${RUN_TESTS} ]]; then
  cargo test --no-default-features
fi

# Build release wheels
python -m maturin build --universal2 --out target/wheels

echo "Package build, trying to import"
echo "Platform:"
python -c "from distutils import util; print(util.get_platform())"
# Verify that it imports properly
python -m pip install openlineage-sql --no-index --find-links target/wheels --force-reinstall
python -c "import openlineage_sql"
echo "all good"