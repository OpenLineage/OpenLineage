#!/bin/bash
#
# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Usage: $ ./up.sh

set -e

# Change working directory to integration
project_root=$(git rev-parse --show-toplevel)
cd "${project_root}"/integration/airflow/tests/integration


mkdir -p failures/airflow/logs
chmod a+rwx -R failures/airflow/logs


case "$1" in
error)
   URL="http://backend:5000/error/"
   ;;
timeout)
   URL="http://backend:5000/timeout/"
   ;;
network-partition)
   URL="http://network-partition:5000/" 
   ;;
*)
   echo "no URL provided"
   ;;
esac

docker-compose -f failures/docker-compose.yml down -v
docker-compose -f failures/docker-compose.yml build
OPENLINEAGE_URL=$URL docker-compose -f failures/docker-compose.yml run integration
docker-compose -f failures/docker-compose.yml logs > failures/airflow/logs/docker.log
docker-compose -f failures/docker-compose.yml down -v