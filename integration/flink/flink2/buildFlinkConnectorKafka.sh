#!/bin/bash
# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

cd /tmp || exit
rm -rf flink-connector-kafka
git clone https://github.com/pawel-big-lebowski/flink-connector-kafka/
cd flink-connector-kafka || exit
git checkout FLINK-36648 # Depends on https://github.com/apache/flink-connector-kafka/pull/140
mvn clean install -DskipTests