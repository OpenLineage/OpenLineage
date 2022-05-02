#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0.


# Build script for OpenLineage SQL parser.
# It's assumed that it will be run in manylinux image: see https://github.com/pypa/manylinux
set -e

# Manylinux image has multiple "pythons" - in /opt/python directory.
# We use Python 3.7, since it's the lowest we want to use
# and create local virtualenv - it's easier to proceed in venv than use python behind long absolute path
/opt/python/cp37-cp37m/bin/python -m venv .env
source .env/bin/activate

# Maturin is build tool that we're using. It can build python wheels based on standard Rust Cargo.toml.
python -m pip install maturin

# Install Rust
curl https://sh.rustup.rs -sSf | sh -s -- -y

# Disable incremental compilation, since it causes issues.
export CARGO_INCREMENTAL=0

# Run test and build release wheels.
source $HOME/.cargo/env && cargo test --no-default-features && maturin build --release