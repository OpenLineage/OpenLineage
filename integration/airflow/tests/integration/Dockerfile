FROM openlineage-airflow-base:latest AS build

FROM bitnami/airflow-scheduler:1.10.15 AS scheduler
COPY --from=build /app/wheel /whl
USER root
RUN apt-get update && \
    apt-get install -y git build-essential
RUN printf "airflow-provider-great-expectations==0.0.8\ngreat-expectations==0.13.42\ndbt-bigquery==0.20.1\noauthlib==2.1.0\n" > /opt/bitnami/airflow/requirements.txt
RUN ls /whl/openlineage* >> /opt/bitnami/airflow/requirements.txt
USER 1001
RUN cd /opt/bitnami/airflow/ && . ./venv/bin/activate && python -m pip install --upgrade pip && pip install -r requirements.txt

FROM bitnami/airflow-worker:1.10.15 AS worker
COPY --from=build /app/wheel /whl
USER root
RUN apt-get update && \
    apt-get install -y git build-essential
RUN printf "airflow-provider-great-expectations==0.0.8\ngreat-expectations==0.13.42\ndbt-bigquery==0.20.1\noauthlib==2.1.0\n" > /opt/bitnami/airflow/requirements.txt
RUN ls /whl/openlineage* >> /opt/bitnami/airflow/requirements.txt
USER 1001
RUN cd /opt/bitnami/airflow/ && . ./venv/bin/activate && python -m pip install --upgrade pip && pip install -r requirements.txt

FROM bitnami/airflow:1.10.15 AS airflow
COPY --from=build /app/wheel /whl
USER root
RUN apt-get update && \
    apt-get install -y git build-essential
RUN printf "airflow-provider-great-expectations==0.0.8\ngreat-expectations==0.13.42\ndbt-bigquery==0.20.1\noauthlib==2.1.0\n" > /opt/bitnami/airflow/requirements.txt
RUN ls /whl/openlineage* >> /opt/bitnami/airflow/requirements.txt
USER 1001
RUN cd /opt/bitnami/airflow/ && . ./venv/bin/activate && python -m pip install --upgrade pip && pip install -r requirements.txt

FROM openlineage-airflow-base:latest AS integration
COPY integration-requirements.txt integration-requirements.txt
COPY *.py ./
COPY requests requests
RUN pip install --use-deprecated=legacy-resolver --user -r integration-requirements.txt
COPY docker/entrypoint.sh entrypoint.sh
COPY docker/wait-for-it.sh wait-for-it.sh
ENTRYPOINT ["/entrypoint.sh"]
