#!/usr/bin/env bash
#
# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#

cd "$(dirname "$0")"
./gradlew javadoc
rm -rf ../../website/static/apidocs/javadoc
mv ./build/docs/javadoc ../../website/static/apidocs/