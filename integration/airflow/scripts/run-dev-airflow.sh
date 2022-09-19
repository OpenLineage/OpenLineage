#!/bin/bash
#
# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Usage: $ ./run-dev-airflow.sh

set -e

# Change working directory to integration
project_root=$(git rev-parse --show-toplevel)
current_dir=$(pwd)

REBUILD="false"
while [[ $# -gt 0 ]]
do
  case $1 in
    -r|--rebuild)
       shift
       REBUILD="true"
       ;;
     *) echo "Unknown argument: $1"
      shift
      ;;
  esac
done


if [[ "${REBUILD}" == "true" ]]; then
   echo "Rebuilding..."
   cd "${project_root}"/integration
   cp -r ../client/python .
   docker build -f airflow/Dockerfile.tests -t openlineage-airflow-base .
   rm -rf python
   cd ${current_dir}
fi

# this makes it easier to keep proper permissions on logs
mkdir -p airflow/logs
chmod a+rwx -R airflow/logs

# maybe overkill
OPENLINEAGE_AIRFLOW_WHL=$(docker run --rm openlineage-airflow-base:latest sh -c "ls /whl/openlineage*.whl")
OPENLINEAGE_AIRFLOW_WHL_ALL=$(docker run --rm openlineage-airflow-base:latest sh -c "ls /whl/*")

# Add revision to requirements.txt
cat > requirements.txt <<EOL
airflow-provider-great-expectations==0.0.8
apache-airflow-providers-snowflake==2.5.1
apache-airflow-providers-postgres==3.0.0
apache-airflow-providers-google==6.7.0
great-expectations==0.13.42
dbt-core==1.0.1
dbt-bigquery==1.0.0
dbt-snowflake==1.0.0
${OPENLINEAGE_AIRFLOW_WHL}
EOL

# Add revision to integration-requirements.txt
cat > integration-requirements.txt <<EOL
requests==2.24.0
psycopg2-binary==2.9.2
mysqlclient>=1.3.6
httplib2>=0.18.1
retrying==1.3.3
pytest==6.2.2
jinja2==3.0.2
python-dateutil==2.8.2
${OPENLINEAGE_AIRFLOW_WHL}
EOL

# string operator: from variable AIRFLOW_IMAGE
#  ##   <-- greedy front trim
#  *    <-- matches anything
#  :    <-- until the last ':'
AIRFLOW_VERSION=${AIRFLOW_IMAGE##*:}

# Remove -python3.7 from the tag
export AIRFLOW_VERSION=${AIRFLOW_VERSION::-10}
export BIGQUERY_PREFIX=$(echo "$AIRFLOW_VERSION" | tr "-" "_" | tr "." "_")
export DBT_DATASET_PREFIX=$(echo "$AIRFLOW_VERSION" | tr "-" "_" | tr "." "_")_dbt
export DAGS_ARE_PAUSED_AT_CREATION=True

YAML_LOCATION="${project_root}"/integration/airflow/tests/integration/tests/docker-compose-2.yml

UP_ARGS=""
if [[ "${REBUILD}" == "true" ]]; then
   UP_ARGS+="--build --force-recreate"
fi

docker-compose -f $YAML_LOCATION down
docker-compose -f $YAML_LOCATION up $UP_ARGS --abort-on-container-exit airflow_init postgres
docker-compose -f $YAML_LOCATION --profile dev up $UP_ARGS --scale airflow_init=0 --scale integration=0
