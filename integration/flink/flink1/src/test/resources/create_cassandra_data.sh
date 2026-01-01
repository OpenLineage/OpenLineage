#!/bin/bash
# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

set -e

cqlsh cassandra-server 9042 -e "CREATE KEYSPACE IF NOT EXISTS flink WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};"
cqlsh cassandra-server 9042 -e "CREATE TABLE IF NOT EXISTS flink.source_event (id UUID PRIMARY KEY, content text, timestamp bigint);"
cqlsh cassandra-server 9042 -e "CREATE TABLE IF NOT EXISTS flink.sink_event (id UUID PRIMARY KEY, content text, timestamp bigint);"
cqlsh cassandra-server 9042 -e "INSERT INTO flink.source_event (id, content, timestamp) VALUES (uuid(), 'started', 12344567);"
cqlsh cassandra-server 9042 -e "INSERT INTO flink.source_event (id, content, timestamp) VALUES (uuid(), 'ended', 12344667);"