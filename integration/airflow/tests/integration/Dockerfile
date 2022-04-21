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
RUN pip install -U setuptools
RUN pip install --user -r requirements.txt
COPY docker/entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint.sh", "/entrypoint"]

FROM python:3.7-slim AS integration
RUN apt-get update && \
    apt-get install -y python3-dev default-libmysqlclient-dev build-essential
RUN mkdir /app
WORKDIR /app
COPY integration-requirements.txt integration-requirements.txt
COPY *.py ./
COPY requests requests
RUN pip install -U setuptools
RUN pip install --use-deprecated=legacy-resolver --user -r integration-requirements.txt
COPY docker/wait-for-it.sh wait-for-it.sh
COPY docker/entrypoint.sh entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]