#!/bin/bash
#
# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

BASEDIR=$(dirname "${BASH_SOURCE[0]}")
source "$BASEDIR"/../../../gradle.properties
if [ -z "$version" ]; then
    echo "Error: version is not set."
    exit 1
fi

JAR=build/libs/openlineage-sql-java-$version.jar

# Prefer JAVA_HOME over PATH, so the smoke test runs on the same JDK as the
# Gradle build, which matters for loading the native library
JAVA_BIN=${JAVA_HOME:+$JAVA_HOME/bin/}

rm -f "$BASEDIR"/*.class
"${JAVA_BIN}javac" -cp "$JAR" "$BASEDIR"/TestParser.java
"${JAVA_BIN}java" -cp "$JAR":"$BASEDIR" TestParser "$@"