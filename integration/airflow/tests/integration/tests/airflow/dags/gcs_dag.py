# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import os

from urllib.parse import urlparse

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.dates import days_ago


GOOGLE_CLOUD_STORAGE_SOURCE_URI = os.environ["GOOGLE_CLOUD_STORAGE_SOURCE_URI"]
SOURCE_PARSED = urlparse(GOOGLE_CLOUD_STORAGE_SOURCE_URI)
SOURCE_BUCKET = SOURCE_PARSED.netloc
SOURCE_OBJECT = SOURCE_PARSED.path

GOOGLE_CLOUD_STORAGE_DESTINATION_URI = os.environ["GOOGLE_CLOUD_STORAGE_DESTINATION_URI"]
DESTINATION_PARSED = urlparse(GOOGLE_CLOUD_STORAGE_DESTINATION_URI)
DESTINATION_BUCKET = DESTINATION_PARSED.netloc
DESTINATION_OBJECT = DESTINATION_PARSED.path


with DAG(dag_id="gcs_dag", start_date=days_ago(7), schedule_interval="@once") as dag:
    GCSToGCSOperator(
        task_id="gcs_task",
        gcp_conn_id="gcs_conn",
        source_bucket=SOURCE_BUCKET,
        source_object=SOURCE_OBJECT[1:],
        destination_bucket=DESTINATION_BUCKET,
        destination_object=DESTINATION_OBJECT[1:],
    )
