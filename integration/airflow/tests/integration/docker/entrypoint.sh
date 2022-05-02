#!/bin/bash

set -x

pip install -U setuptools

(cd /opt/openlineage/sql && pip install target/wheels/*.whl)

(cd /opt/openlineage/python && pip install --no-deps -e .)

(cd /opt/openlineage/common && pip install --no-deps -e .)

(cd /opt/openlineage/airflow && pip install --no-deps -e .)

(cd /opt/openlineage/dbt && pip install --user .)

ENTRYPOINT=$1
shift

exec $ENTRYPOINT "${@}"