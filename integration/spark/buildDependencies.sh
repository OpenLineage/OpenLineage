#!/bin/bash
# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

set -x -e
(cd ../../client/java && ./gradlew clean publishToMavenLocal)
(cd ../spark-extension-interfaces && ./gradlew clean publishToMavenLocal)
(cd ../sql/iface-java && ./script/compile.sh && ./script/build.sh)
