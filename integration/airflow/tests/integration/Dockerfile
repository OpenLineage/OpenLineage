FROM openlineage-airflow-base:latest AS build

FROM bitnami/airflow-scheduler:1.10.12 AS scheduler
COPY --from=build /app/wheel /whl
USER root
RUN apt-get update && \
    apt-get install -y git build-essential
USER 1001


FROM bitnami/airflow-worker:1.10.12 AS worker
COPY --from=build /app/wheel /whl
USER root
RUN apt-get update && \
    apt-get install -y git build-essential
USER 1001

FROM bitnami/airflow:1.10.12 AS airflow
COPY --from=build /app/wheel /whl
USER root
RUN apt-get update && \
    apt-get install -y git build-essential
USER 1001

FROM openlineage-airflow-base:latest AS integration
COPY integration-requirements.txt integration-requirements.txt
COPY *.py ./
COPY requests requests
RUN pip install --use-deprecated=legacy-resolver --user -r integration-requirements.txt
COPY docker/entrypoint.sh entrypoint.sh
COPY docker/wait-for-it.sh wait-for-it.sh
ENTRYPOINT ["/entrypoint.sh"]
