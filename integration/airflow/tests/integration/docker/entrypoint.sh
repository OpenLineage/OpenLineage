#!/bin/bash

set -x

pip install -U setuptools

(cd /opt/openlineage/python && pip install --no-deps -e .)

(cd /opt/openlineage/common && pip install --no-deps -e .)

(cd /opt/openlineage/airflow && pip install --no-deps -e .)

ENTRYPOINT=$1
shift

exec $ENTRYPOINT "${@}"