#!/bin/bash
#
# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Usage: $ ./init-db-mysql.sh

set -eu

mysql --user="${MYSQL_USER}" --password="${MYSQL_PASSWORD}" "${MYSQL_DATABASE}" > /dev/null <<-EOSQL
  CREATE TABLE IF NOT EXISTS top_delivery_times (
      order_id            INTEGER,
      order_placed_on     TIMESTAMP NOT NULL,
      order_dispatched_on TIMESTAMP NOT NULL,
      order_delivered_on  TIMESTAMP NOT NULL,
      order_delivery_time DOUBLE PRECISION NOT NULL,
      customer_email      VARCHAR(64) NOT NULL,
      restaurant_id       INTEGER,
      driver_id           INTEGER
    );
EOSQL
