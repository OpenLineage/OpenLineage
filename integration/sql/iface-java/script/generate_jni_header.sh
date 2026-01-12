#!/bin/bash

# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# This script generates an expected C header that our Rust API
# should adhere to.

BASEDIR=$(dirname "${BASH_SOURCE[0]}")
SRC=$BASEDIR/../src
DEPENDENCIES=$BASEDIR/../build/dependencies
JAVA_SRC=$SRC/main/java/io/openlineage/sql
javac -cp "$DEPENDENCIES/*" -h "$SRC"/main/jni "$JAVA_SRC"/*.java
