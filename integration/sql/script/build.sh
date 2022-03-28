#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0.

set -e
/opt/python/cp37-cp37m/bin/python -m pip install maturin
curl https://sh.rustup.rs -sSf | sh -s -- -y
source $HOME/.cargo/env && /opt/python/cp37-cp37m/bin/maturin build