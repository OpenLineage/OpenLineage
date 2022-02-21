FROM openlineage-airflow-base:latest AS build

FROM apache/airflow:2.2.3 AS airflow
COPY --from=build /app/wheel /whl
USER airflow
COPY requirements.txt requirements.txt
RUN pip install --user -r requirements.txt

FROM openlineage-airflow-base:latest AS integration
COPY integration-requirements.txt integration-requirements.txt
COPY *.py ./
RUN pip install --use-deprecated=legacy-resolver --user -r integration-requirements.txt
COPY docker/entrypoint.sh entrypoint.sh
COPY docker/wait-for-it.sh wait-for-it.sh
ENTRYPOINT ["/entrypoint.sh"]
