#!/bin/sh

VOLUME_PREFIX=$1

UTILS_VOLUME="${VOLUME_PREFIX}_utils"
DB_INIT_VOLUME="${VOLUME_PREFIX}_db-init"
MYSQL_INIT_VOLUME="${VOLUME_PREFIX}_mysql-init"
DAGS_VOLUME="${VOLUME_PREFIX}_airflow_dags"
FAILED_DAGS_VOLUME="${VOLUME_PREFIX}_airflow_failed_dags"
CONFIG_VOLUME="${VOLUME_PREFIX}_airflow_config"
LOGS_VOLUME="${VOLUME_PREFIX}_airflow_logs"
LIBS_VOLUME="${VOLUME_PREFIX}_openlineage"

docker volume create $UTILS_VOLUME
docker volume create $DB_INIT_VOLUME
docker volume create $MYSQL_INIT_VOLUME
docker volume create $DAGS_VOLUME
docker volume create $FAILED_DAGS_VOLUME
docker volume create CONFIG_VOLUME
docker volume create $LOGS_VOLUME
docker volume create $LIBS_VOLUME
docker create --name openlineage-volume-helper \
  -v $LIBS_VOLUME:/opt/openlineage \
  -v $UTILS_VOLUME:/opt/openlineage-utils \
  -v $DB_INIT_VOLUME:/opt/openlineage-db-init \
  -v $MYSQL_INIT_VOLUME:/opt/openlineage-mysql-init \
  -v $DAGS_VOLUME:/opt/airflow/dags \
  -v $DAGS_VOLUME:/opt/airflow/config \
  -v $FAILED_DAGS_VOLUME:/opt/airflow/failed_dags busybox

docker cp ./docker/wait-for-it.sh openlineage-volume-helper:/opt/openlineage-utils/wait-for-it.sh
docker cp gcloud openlineage-volume-helper:/opt/openlineage-utils/

docker cp ./docker/init-db.sh openlineage-volume-helper:/opt/openlineage-db-init/init-db.sh
docker cp ./docker/init-db-mysql.sh openlineage-volume-helper:/opt/openlineage-mysql-init/init-db-mysql.sh

docker cp tests/airflow/config openlineage-volume-helper:/opt/airflow/
docker cp tests/airflow/dags openlineage-volume-helper:/opt/airflow/

docker cp failures/airflow/dags openlineage-volume-helper:/opt/airflow/failed_dags

docker cp ../../../airflow openlineage-volume-helper:/opt/openlineage/
docker cp ../../../common openlineage-volume-helper:/opt/openlineage/
docker cp ../../../dbt openlineage-volume-helper:/opt/openlineage/
docker cp ../../../../client/python openlineage-volume-helper:/opt/openlineage/

docker rm openlineage-volume-helper
