ARG AIRFLOW_IMAGE=apache/airflow:2.4.3-python3.8
FROM $AIRFLOW_IMAGE AS airflow
COPY integration/dbt /app/openlineage/integration/dbt
COPY integration/common /app/openlineage/integration/common
COPY integration/airflow /app/openlineage/integration/airflow
COPY integration/target /app/openlineage/integration/sql/target
COPY client/python /app/openlineage/client/python
USER root
RUN mkdir -p /opt/data
RUN apt-get update && apt-get install -y git
COPY integration/airflow/tests/integration/data /opt/data
COPY integration/airflow/tests/integration/docker/wait-for-it.sh /opt/data/wait-for-it.sh
RUN chmod -R 777 /opt/data
RUN chmod a+rwxX -R /app/openlineage
USER airflow
RUN AIRFLOW_VERSION=`airflow version` && \
    PYTHON_VERSION=`python -c "import sys; print('{}.{}'.format(*sys.version_info))"` && \
    cd /app/openlineage/integration/sql && \
    pip install openlineage-sql --no-index --find-links target/wheels --force-reinstall && \
    cd ../airflow && \
    pip install --no-cache-dir --use-deprecated=legacy-resolver -r dev-requirements.txt \
    --constraint=https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-$PYTHON_VERSION.txt
RUN python -m virtualenv dbt_venv && pwd && source dbt_venv/bin/activate && \
    unset PIP_USER && \
    python -m pip install pip --upgrade && \
    cd /app/openlineage/integration/sql && \
    pip install openlineage-sql --no-index --find-links target/wheels --force-reinstall && \
    cd ../airflow && \
    python -m pip install -r tests/integration/tests/airflow/dbt-requirements.txt && deactivate  # We need separate env to run dbt due to dependency conflicts
