#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
