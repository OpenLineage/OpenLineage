#!/bin/bash
#
# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

BASEDIR=$(dirname "${BASH_SOURCE[0]}")
source "$BASEDIR"/../../../gradle.properties
if [ -z "$version" ]; then
    echo "Error: version is not set."
    exit 1
fi

JAR=build/libs/openlineage-sql-java-$version.jar

rm -f "$BASEDIR"/*.class
javac -cp "$JAR" "$BASEDIR"/TestParser.java
java -cp "$JAR":"$BASEDIR" TestParser "$@"