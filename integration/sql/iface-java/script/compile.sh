#!/bin/bash

# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

set -e

RED='\033[0;31m'
NC='\033[0m'

if ! [ -x "$(command -v cargo)" ]; then
  # Install Rust
  curl https://sh.rustup.rs -sSf | sh -s -- -y
  source "$HOME"/.cargo/env
fi

BASEDIR=$(dirname "${BASH_SOURCE[0]}")
ROOT=$BASEDIR/..

if [[ "$OSTYPE" == "linux-"* && "$(uname -m)" == "x86_64" ]]; then
    NATIVE_LIB_NAME=libopenlineage_sql_java_x86_64.so
elif [[ "$OSTYPE" == "linux-"* && "$(uname -m)" == "aarch64" ]]; then
    NATIVE_LIB_NAME=libopenlineage_sql_java_aarch64.so
elif [[ "$OSTYPE" == "darwin"* && "$(uname -m)" == "arm64" ]]; then
    NATIVE_LIB_NAME=libopenlineage_sql_java_arm64.dylib
elif [[ "$OSTYPE" == "darwin"* ]]; then
    NATIVE_LIB_NAME=libopenlineage_sql_java.dylib
else
    printf "\n%sUnsupported OS %s!\n%s\n" "$RED" "$OSTYPE" "$NC"
fi

# Build the Rust bindings
rm -rf build/libs/*
cd "$ROOT"/..
cargo clean
cargo build -p openlineage_sql_java

shopt -s extglob
mv target/debug/libopenlineage_sql_java*(*.so|*.dylib) target/debug/"$NATIVE_LIB_NAME"
