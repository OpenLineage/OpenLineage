#!/bin/bash

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

# Install Rust
curl https://sh.rustup.rs -sSf | sh -s -- -y
source $HOME/.cargo/env

BASEDIR=$(dirname $BASH_SOURCE)
ROOT=$BASEDIR/..
SRC=$ROOT/src
JAVA_SRC=$SRC/java/io/openlineage/sql
RESOURCES=$ROOT/src/main/resources/io/openlineage/sql
SCRIPTS=$ROOT/script

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    NATIVE_LIB_NAME=libopenlineage_sql_java.so
elif [[ "$OSTYPE" == "darwin"* ]]; then
    NATIVE_LIB_NAME=libopenlineage_sql_java.dylib
else
    printf "\n${RED}Unsupported OS!\n${NC}"
fi

# Build the Rust bindings
rm -rf build/libs/*
cd $ROOT/..
cargo build -p openlineage_sql_java
