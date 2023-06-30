#!/usr/bin/env bash
#
# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Install script for building OpenLineage SQL parser.
# It's assumed that it will be run on MacOS
set -e

echo "Installing homebrew"
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

export HOMEBREW_NO_AUTO_UPDATE=1
export HOMEBREW_NO_INSTALL_CLEANUP=1

echo "Installing Python 3.8"
brew install python@3.8

echo "Installing Rust"
curl https://sh.rustup.rs -sSf | sh -s -- -y

source $HOME/.cargo/env

rustup target add aarch64-apple-darwin
rustup target add x86_64-apple-darwin

# Maturin is build tool that we're using. It can build python wheels based on standard Rust Cargo.toml.
echo "Installing Maturin"
if [ -x "$(command -v /usr/local/opt/python@3.8/bin/python3)" ]; then
  /usr/local/opt/python@3.8/bin/python3 -m pip install maturin
elif [ -x "$(command -v /usr/local/bin/python3.8)" ]; then
  /usr/local/bin/python3.8 -m pip install maturin
elif [ -x "$(command -v python3.8)" ]; then
  python3.8 -m pip install maturin
else
  python -m pip install maturin
fi
