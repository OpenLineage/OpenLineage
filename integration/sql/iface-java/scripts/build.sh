#!/bin/bash

CURRENT_DIR=$(pwd)
BUILD=iface-java/build
SRC=iface-java/src
JAVA_SRC=$SRC/java/io/openlineage/sql

# Let's generate this header every run so that it is always
# up to date.
./iface-java/scripts/generate_jni_header.sh

# Build the Rust bindings
cargo build -p openlineage_sql_java

export LD_LIBRARY_PATH="$CURRENT_DIR/target/debug/"
cd iface-java
mvn package
