FROM openlineage-airflow-base:latest AS build

FROM apache/airflow:1.10.12-python3.6 AS airflow
COPY --from=build /app/wheel /whl
USER root
RUN apt-get update && \
    apt-get install -y git
USER airflow
RUN pip install /whl/*

FROM openlineage-airflow-base:latest AS integration
COPY integration-requirements.txt integration-requirements.txt
COPY integration.py integration.py
RUN pip install --use-deprecated=legacy-resolver --user -r integration-requirements.txt
COPY docker/entrypoint.sh entrypoint.sh
COPY docker/wait-for-it.sh wait-for-it.sh
ENTRYPOINT ["/entrypoint.sh"]
