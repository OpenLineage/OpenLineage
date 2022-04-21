FROM openlineage-airflow-base:latest AS build

FROM apache/airflow:1.10.15-python3.7 AS airflow
USER root
RUN sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 467B942D3A79BD29  # Mysql fix for https://github.com/apache/airflow/issues/20911
RUN apt-get update && \
    apt-get install -y git build-essential
RUN mkdir -p /opt/airflow
COPY data /opt/data
RUN chown -R airflow:airflow /opt/airflow
RUN chmod -R 777 /opt/data
USER airflow
COPY requirements.txt requirements.txt
COPY --from=build /whl /whl
RUN pip install --user -r requirements.txt