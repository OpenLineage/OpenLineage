#!/usr/bin/env bash
#
# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Install script for building OpenLineage SQL parser.
# It's assumed that it will be run on MacOS
set -e

echo "Installing homebrew"
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

export HOMEBREW_NO_AUTO_UPDATE=1
export HOMEBREW_NO_INSTALL_CLEANUP=1

echo "Installing Python 3.7"
brew install python@3.7

echo "Installing Rust"
curl https://sh.rustup.rs -sSf | sh -s -- -y

source $HOME/.cargo/env

rustup target add aarch64-apple-darwin

# Maturin is build tool that we're using. It can build python wheels based on standard Rust Cargo.toml.
echo "Installing Maturin"
python3.7 -m pip install maturin
