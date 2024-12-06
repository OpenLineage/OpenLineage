#!/bin/bash

# Exit on error
set -e 

# Set password for database access
export PASSWORD='password'

# Initialize databases with schema
psql -U admin -d test -f create-databases.sql
psql -U hiveuser -d metastore23 -f metastore-2.3.0.sql
psql -U hiveuser -d metastore31 -f metastore-3.1.0.sql