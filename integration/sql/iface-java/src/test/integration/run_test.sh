#!/bin/bash
#
# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

BASEDIR=$(dirname $BASH_SOURCE)
source $BASEDIR/../../../gradle.properties
JAR=build/libs/openlineage-sql-java-$version.jar

rm -f $BASEDIR/*.class
javac -cp $JAR $BASEDIR/TestParser.java
java -cp $JAR:$BASEDIR TestParser "$@"