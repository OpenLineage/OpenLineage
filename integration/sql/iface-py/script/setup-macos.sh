#!/usr/bin/env bash
#
# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Install script for building OpenLineage SQL parser.
# It's assumed that it will be run on MacOS
set -e

echo "Installing homebrew"
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

export HOMEBREW_NO_AUTO_UPDATE=1
export HOMEBREW_NO_INSTALL_CLEANUP=1

echo "Installing Rust"
curl https://sh.rustup.rs -sSf | sh -s -- -y

echo "Installing uv"
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env
source $HOME/.cargo/env

rustup target add aarch64-apple-darwin
rustup target add x86_64-apple-darwin

echo "Installing Python 3.8"
uv python install 3.8

# Maturin is build tool that we're using. It can build python wheels based on standard Rust Cargo.toml.
echo "Installing Maturin"
echo "$PWD"
(cd iface-py && uv sync --no-install-project)
