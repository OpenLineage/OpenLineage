#!/bin/bash
#
# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Usage: $ ./init-db.sh

set -eu

# STEP 1: Add users, databases, etc
psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" > /dev/null <<-EOSQL
  CREATE USER ${AIRFLOW_USER};
  ALTER USER ${AIRFLOW_USER} WITH PASSWORD '${AIRFLOW_PASSWORD}';
  CREATE DATABASE ${AIRFLOW_DB};
  GRANT ALL PRIVILEGES ON DATABASE ${AIRFLOW_DB} TO ${AIRFLOW_USER};
  CREATE USER ${FOOD_DELIVERY_USER};
  ALTER USER ${FOOD_DELIVERY_USER} WITH PASSWORD '${FOOD_DELIVERY_PASSWORD}';
  CREATE DATABASE ${FOOD_DELIVERY_DB};
  GRANT ALL PRIVILEGES ON DATABASE ${FOOD_DELIVERY_DB} TO ${FOOD_DELIVERY_USER};
EOSQL

# STEP 2: Add tables
psql -v ON_ERROR_STOP=1 --username "${FOOD_DELIVERY_USER}" > /dev/null <<-EOSQL
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
