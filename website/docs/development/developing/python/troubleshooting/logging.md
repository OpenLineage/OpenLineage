---
title: Logging
sidebar_position: 1
---

OpenLineage uses python's [logging facility](https://docs.python.org/3/library/logging.html) when generating logs. Being able to emit logs for various purposes is very helpful when troubleshooting OpenLineage.

Consider the following sample python script that emits OpenLineage events:

```python
#!/usr/bin/env python3
from openlineage.client.run import (
    RunEvent,
    RunState,
    Run,
    Job,
    Dataset,
    OutputDataset,
    InputDataset,
)
from openlineage.client.client import OpenLineageClient, OpenLineageClientOptions
from openlineage.client.facet import (
    SqlJobFacet,
    SchemaDatasetFacet,
    SchemaField,
    OutputStatisticsOutputDatasetFacet,
    SourceCodeLocationJobFacet,
    NominalTimeRunFacet,
    DataQualityMetricsInputDatasetFacet,
    ColumnMetric,
)
from openlineage.client.uuid import generate_new_uuid
from datetime import datetime, timezone, timedelta
import time
from random import random

PRODUCER = f"https://github.com/openlineage-user"
namespace = "python_client"

url = "http://localhost:5000"
api_key = "1234567890ckcu028rzu5l"

client = OpenLineageClient(
    url=url,
	# optional api key in case the backend requires it
    options=OpenLineageClientOptions(api_key=api_key),
)

# generates job facet
def job(job_name, sql, location):
    facets = {"sql": SqlJobFacet(sql)}
    if location != None:
        facets.update(
            {"sourceCodeLocation": SourceCodeLocationJobFacet("git", location)}
        )
    return Job(namespace=namespace, name=job_name, facets=facets)


# generates run racet
def run(run_id, hour):
    return Run(
        runId=run_id,
        facets={
            "nominalTime": NominalTimeRunFacet(
                nominalStartTime=f"2022-04-14T{twoDigits(hour)}:12:00Z"
            )
        },
    )


# generates dataset
def dataset(name, schema=None, ns=namespace):
    if schema == None:
        facets = {}
    else:
        facets = {"schema": schema}
    return Dataset(namespace, name, facets)


# generates output dataset
def outputDataset(dataset, stats):
    output_facets = {"stats": stats, "outputStatistics": stats}
    return OutputDataset(dataset.namespace, dataset.name, dataset.facets, output_facets)


# generates input dataset
def inputDataset(dataset, dq):
    input_facets = {
        "dataQuality": dq,
    }
    return InputDataset(dataset.namespace, dataset.name, dataset.facets, input_facets)


def twoDigits(n):
    if n < 10:
        result = f"0{n}"
    elif n < 100:
        result = f"{n}"
    else:
        raise f"error: {n}"
    return result


now = datetime.now(timezone.utc)


# generates run Event
def runEvents(job_name, sql, inputs, outputs, hour, min, location, duration):
    run_id = str(generate_new_uuid())
    myjob = job(job_name, sql, location)
    myrun = run(run_id, hour)
    started_at = now + timedelta(hours=hour, minutes=min, seconds=20 + round(random() * 10))
    ended_at = started_at + timedelta(minutes=duration, seconds=20 + round(random() * 10))
    return (
        RunEvent(
            eventType=RunState.START,
            eventTime=started_at.isoformat(),
            run=myrun,
            job=myjob,
            producer=PRODUCER,
            inputs=inputs,
            outputs=outputs,
        ),
        RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=ended_at.isoformat(),
            run=myrun,
            job=myjob,
            producer=PRODUCER,
            inputs=inputs,
            outputs=outputs,
        ),
    )


# add run event to the events list
def addRunEvents(
    events, job_name, sql, inputs, outputs, hour, minutes, location=None, duration=2
):
    (start, complete) = runEvents(
        job_name, sql, inputs, outputs, hour, minutes, location, duration
    )
    events.append(start)
    events.append(complete)


events = []

# create dataset data
for i in range(0, 5):

    user_counts = dataset("tmp_demo.user_counts")
    user_history = dataset(
        "temp_demo.user_history",
        SchemaDatasetFacet(
            fields=[
                SchemaField(name="id", type="BIGINT", description="the user id"),
                SchemaField(
                    name="email_domain", type="VARCHAR", description="the user id"
                ),
                SchemaField(name="status", type="BIGINT", description="the user id"),
                SchemaField(
                    name="created_at",
                    type="DATETIME",
                    description="date and time of creation of the user",
                ),
                SchemaField(
                    name="updated_at",
                    type="DATETIME",
                    description="the last time this row was updated",
                ),
                SchemaField(
                    name="fetch_time_utc",
                    type="DATETIME",
                    description="the time the data was fetched",
                ),
                SchemaField(
                    name="load_filename",
                    type="VARCHAR",
                    description="the original file this data was ingested from",
                ),
                SchemaField(
                    name="load_filerow",
                    type="INT",
                    description="the row number in the original file",
                ),
                SchemaField(
                    name="load_timestamp",
                    type="DATETIME",
                    description="the time the data was ingested",
                ),
            ]
        ),
        "snowflake://",
    )

    create_user_counts_sql = """CREATE OR REPLACE TABLE TMP_DEMO.USER_COUNTS AS (
			SELECT DATE_TRUNC(DAY, created_at) date, COUNT(id) as user_count
			FROM TMP_DEMO.USER_HISTORY
			GROUP BY date
			)"""

    # location of the source code
    location = "https://github.com/some/airflow/dags/example/user_trends.py"

    # run simulating Airflow DAG with snowflake operator
    addRunEvents(
        events,
        "create_user_counts",
        create_user_counts_sql,
        [user_history],
        [user_counts],
        i,
        11,
        location,
    )


for event in events:
    from openlineage.client.serde import Serde
    client.emit(event)

```

When you use OpenLineage backend such as Marquez on your local environment, the script would emit OpenLienage events to it.

```bash
python oltest.py
```

However, this short script does not produce any logging information, as the logging configuration is not setup.

Add the following line to `oltest.py`, to configure the logging level as `DEBUG`.

```python
...
import logging
...
logging.basicConfig(level=logging.DEBUG)
...
```

Re-running the `oltest.py` again will now produce the following outputs:

```
DEBUG:openlineage.client.transport.http:Constructing openlineage client to send events to http://localhost:5000
DEBUG:openlineage.client.transport.http:Sending openlineage event {"eventTime": "2022-12-07T02:10:24.369600+00:00", "eventType": "START", "inputs": [{"facets": {"schema": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SchemaDatasetFacet", "fields": [{"description": "the user id", "name": "id", "type": "BIGINT"}, {"description": "the user id", "name": "email_domain", "type": "VARCHAR"}, {"description": "the user id", "name": "status", "type": "BIGINT"}, {"description": "date and time of creation of the user", "name": "created_at", "type": "DATETIME"}, {"description": "the last time this row was updated", "name": "updated_at", "type": "DATETIME"}, {"description": "the time the data was fetched", "name": "fetch_time_utc", "type": "DATETIME"}, {"description": "the original file this data was ingested from", "name": "load_filename", "type": "VARCHAR"}, {"description": "the row number in the original file", "name": "load_filerow", "type": "INT"}, {"description": "the time the data was ingested", "name": "load_timestamp", "type": "DATETIME"}]}}, "name": "temp_demo.user_history", "namespace": "python_client"}], "job": {"facets": {"sourceCodeLocation": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SourceCodeLocationJobFacet", "type": "git", "url": "https://github.com/some/airflow/dags/example/user_trends.py"}, "sql": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SqlJobFacet", "query": "CREATE OR REPLACE TABLE TMP_DEMO.USER_COUNTS AS (\n\t\t\tSELECT DATE_TRUNC(DAY, created_at) date, COUNT(id) as user_count\n\t\t\tFROM TMP_DEMO.USER_HISTORY\n\t\t\tGROUP BY date\n\t\t\t)"}}, "name": "create_user_counts", "namespace": "python_client"}, "outputs": [{"facets": {}, "name": "tmp_demo.user_counts", "namespace": "python_client"}], "producer": "https://github.com/openlineage-user", "run": {"facets": {"nominalTime": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet", "nominalStartTime": "2022-04-14T00:12:00Z"}}, "runId": "e74f805a-0fde-4480-84a3-6919011eb14d"}}
DEBUG:urllib3.connectionpool:Starting new HTTP connection (1): localhost:5000
DEBUG:urllib3.connectionpool:http://localhost:5000 "POST /api/v1/lineage HTTP/1.1" 201 0
DEBUG:openlineage.client.transport.http:Sending openlineage event {"eventTime": "2022-12-07T02:12:47.369600+00:00", "eventType": "COMPLETE", "inputs": [{"facets": {"schema": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SchemaDatasetFacet", "fields": [{"description": "the user id", "name": "id", "type": "BIGINT"}, {"description": "the user id", "name": "email_domain", "type": "VARCHAR"}, {"description": "the user id", "name": "status", "type": "BIGINT"}, {"description": "date and time of creation of the user", "name": "created_at", "type": "DATETIME"}, {"description": "the last time this row was updated", "name": "updated_at", "type": "DATETIME"}, {"description": "the time the data was fetched", "name": "fetch_time_utc", "type": "DATETIME"}, {"description": "the original file this data was ingested from", "name": "load_filename", "type": "VARCHAR"}, {"description": "the row number in the original file", "name": "load_filerow", "type": "INT"}, {"description": "the time the data was ingested", "name": "load_timestamp", "type": "DATETIME"}]}}, "name": "temp_demo.user_history", "namespace": "python_client"}], "job": {"facets": {"sourceCodeLocation": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SourceCodeLocationJobFacet", "type": "git", "url": "https://github.com/some/airflow/dags/example/user_trends.py"}, "sql": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SqlJobFacet", "query": "CREATE OR REPLACE TABLE TMP_DEMO.USER_COUNTS AS (\n\t\t\tSELECT DATE_TRUNC(DAY, created_at) date, COUNT(id) as user_count\n\t\t\tFROM TMP_DEMO.USER_HISTORY\n\t\t\tGROUP BY date\n\t\t\t)"}}, "name": "create_user_counts", "namespace": "python_client"}, "outputs": [{"facets": {}, "name": "tmp_demo.user_counts", "namespace": "python_client"}], "producer": "https://github.com/openlineage-user", "run": {"facets": {"nominalTime": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet", "nominalStartTime": "2022-04-14T00:12:00Z"}}, "runId": "e74f805a-0fde-4480-84a3-6919011eb14d"}}
DEBUG:urllib3.connectionpool:http://localhost:5000 "POST /api/v1/lineage HTTP/1.1" 201 0
DEBUG:openlineage.client.transport.http:Sending openlineage event {"eventTime": "2022-12-07T03:10:20.369600+00:00", "eventType": "START", "inputs": [{"facets": {"schema": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SchemaDatasetFacet", "fields": [{"description": "the user id", "name": "id", "type": "BIGINT"}, {"description": "the user id", "name": "email_domain", "type": "VARCHAR"}, {"description": "the user id", "name": "status", "type": "BIGINT"}, {"description": "date and time of creation of the user", "name": "created_at", "type": "DATETIME"}, {"description": "the last time this row was updated", "name": "updated_at", "type": "DATETIME"}, {"description": "the time the data was fetched", "name": "fetch_time_utc", "type": "DATETIME"}, {"description": "the original file this data was ingested from", "name": "load_filename", "type": "VARCHAR"}, {"description": "the row number in the original file", "name": "load_filerow", "type": "INT"}, {"description": "the time the data was ingested", "name": "load_timestamp", "type": "DATETIME"}]}}, "name": "temp_demo.user_history", "namespace": "python_client"}], "job": {"facets": {"sourceCodeLocation": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SourceCodeLocationJobFacet", "type": "git", "url": "https://github.com/some/airflow/dags/example/user_trends.py"}, "sql": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SqlJobFacet", "query": "CREATE OR REPLACE TABLE TMP_DEMO.USER_COUNTS AS (\n\t\t\tSELECT DATE_TRUNC(DAY, created_at) date, COUNT(id) as user_count\n\t\t\tFROM TMP_DEMO.USER_HISTORY\n\t\t\tGROUP BY date\n\t\t\t)"}}, "name": "create_user_counts", "namespace": "python_client"}, "outputs": [{"facets": {}, "name": "tmp_demo.user_counts", "namespace": "python_client"}], "producer": "https://github.com/openlineage-user", "run": {"facets": {"nominalTime": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet", "nominalStartTime": "2022-04-14T01:12:00Z"}}, "runId": "ff034dc3-e3e9-4e4b-bcf1-efba104ac4d4"}}
DEBUG:urllib3.connectionpool:http://localhost:5000 "POST /api/v1/lineage HTTP/1.1" 201 0
DEBUG:openlineage.client.transport.http:Sending openlineage event {"eventTime": "2022-12-07T03:12:42.369600+00:00", "eventType": "COMPLETE", "inputs": [{"facets": {"schema": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SchemaDatasetFacet", "fields": [{"description": "the user id", "name": "id", "type": "BIGINT"}, {"description": "the user id", "name": "email_domain", "type": "VARCHAR"}, {"description": "the user id", "name": "status", "type": "BIGINT"}, {"description": "date and time of creation of the user", "name": "created_at", "type": "DATETIME"}, {"description": "the last time this row was updated", "name": "updated_at", "type": "DATETIME"}, {"description": "the time the data was fetched", "name": "fetch_time_utc", "type": "DATETIME"}, {"description": "the original file this data was ingested from", "name": "load_filename", "type": "VARCHAR"}, {"description": "the row number in the original file", "name": "load_filerow", "type": "INT"}, {"description": "the time the data was ingested", "name": "load_timestamp", "type": "DATETIME"}]}}, "name": "temp_demo.user_history", "namespace": "python_client"}], "job": {"facets": {"sourceCodeLocation": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SourceCodeLocationJobFacet", "type": "git", "url": "https://github.com/some/airflow/dags/example/user_trends.py"}, "sql": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SqlJobFacet", "query": "CREATE OR REPLACE TABLE TMP_DEMO.USER_COUNTS AS (\n\t\t\tSELECT DATE_TRUNC(DAY, created_at) date, COUNT(id) as user_count\n\t\t\tFROM TMP_DEMO.USER_HISTORY\n\t\t\tGROUP BY date\n\t\t\t)"}}, "name": "create_user_counts", "namespace": "python_client"}, "outputs": [{"facets": {}, "name": "tmp_demo.user_counts", "namespace": "python_client"}], "producer": "https://github.com/openlineage-user", "run": {"facets": {"nominalTime": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet", "nominalStartTime": "2022-04-14T01:12:00Z"}}, "runId": "ff034dc3-e3e9-4e4b-bcf1-efba104ac4d4"}}
DEBUG:urllib3.connectionpool:http://localhost:5000 "POST /api/v1/lineage HTTP/1.1" 201 0
DEBUG:openlineage.client.transport.http:Sending openlineage event {"eventTime": "2022-12-07T04:10:22.369600+00:00", "eventType": "START", "inputs": [{"facets": {"schema": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SchemaDatasetFacet", "fields": [{"description": "the user id", "name": "id", "type": "BIGINT"}, {"description": "the user id", "name": "email_domain", "type": "VARCHAR"}, {"description": "the user id", "name": "status", "type": "BIGINT"}, {"description": "date and time of creation of the user", "name": "created_at", "type": "DATETIME"}, {"description": "the last time this row was updated", "name": "updated_at", "type": "DATETIME"}, {"description": "the time the data was fetched", "name": "fetch_time_utc", "type": "DATETIME"}, {"description": "the original file this data was ingested from", "name": "load_filename", "type": "VARCHAR"}, {"description": "the row number in the original file", "name": "load_filerow", "type": "INT"}, {"description": "the time the data was ingested", "name": "load_timestamp", "type": "DATETIME"}]}}, "name": "temp_demo.user_history", "namespace": "python_client"}], "job": {"facets": {"sourceCodeLocation": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SourceCodeLocationJobFacet", "type": "git", "url": "https://github.com/some/airflow/dags/example/user_trends.py"}, "sql": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SqlJobFacet", "query": "CREATE OR REPLACE TABLE TMP_DEMO.USER_COUNTS AS (\n\t\t\tSELECT DATE_TRUNC(DAY, created_at) date, COUNT(id) as user_count\n\t\t\tFROM TMP_DEMO.USER_HISTORY\n\t\t\tGROUP BY date\n\t\t\t)"}}, "name": "create_user_counts", "namespace": "python_client"}, "outputs": [{"facets": {}, "name": "tmp_demo.user_counts", "namespace": "python_client"}], "producer": "https://github.com/openlineage-user", "run": {"facets": {"nominalTime": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet", "nominalStartTime": "2022-04-14T02:12:00Z"}}, "runId": "b7304cdf-7c9e-4183-bd9d-1474cb86bad3"}}
DEBUG:urllib3.connectionpool:http://localhost:5000 "POST /api/v1/lineage HTTP/1.1" 201 0
DEBUG:openlineage.client.transport.http:Sending openlineage event {"eventTime": "2022-12-07T04:12:45.369600+00:00", "eventType": "COMPLETE", "inputs": [{"facets": {"schema": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SchemaDatasetFacet", "fields": [{"description": "the user id", "name": "id", "type": "BIGINT"}, {"description": "the user id", "name": "email_domain", "type": "VARCHAR"}, {"description": "the user id", "name": "status", "type": "BIGINT"}, {"description": "date and time of creation of the user", "name": "created_at", "type": "DATETIME"}, {"description": "the last time this row was updated", "name": "updated_at", "type": "DATETIME"}, {"description": "the time the data was fetched", "name": "fetch_time_utc", "type": "DATETIME"}, {"description": "the original file this data was ingested from", "name": "load_filename", "type": "VARCHAR"}, {"description": "the row number in the original file", "name": "load_filerow", "type": "INT"}, {"description": "the time the data was ingested", "name": "load_timestamp", "type": "DATETIME"}]}}, "name": "temp_demo.user_history", "namespace": "python_client"}], "job": {"facets": {"sourceCodeLocation": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SourceCodeLocationJobFacet", "type": "git", "url": "https://github.com/some/airflow/dags/example/user_trends.py"}, "sql": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SqlJobFacet", "query": "CREATE OR REPLACE TABLE TMP_DEMO.USER_COUNTS AS (\n\t\t\tSELECT DATE_TRUNC(DAY, created_at) date, COUNT(id) as user_count\n\t\t\tFROM TMP_DEMO.USER_HISTORY\n\t\t\tGROUP BY date\n\t\t\t)"}}, "name": "create_user_counts", "namespace": "python_client"}, "outputs": [{"facets": {}, "name": "tmp_demo.user_counts", "namespace": "python_client"}], "producer": "https://github.com/openlineage-user", "run": {"facets": {"nominalTime": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet", "nominalStartTime": "2022-04-14T02:12:00Z"}}, "runId": "b7304cdf-7c9e-4183-bd9d-1474cb86bad3"}}
DEBUG:urllib3.connectionpool:http://localhost:5000 "POST /api/v1/lineage HTTP/1.1" 201 0
....
```

DEBUG will also produce meaningful error messages when something does not work correctly. For example, if the backend server does not exist, you would get the following messages in your console output:

```
DEBUG:openlineage.client.transport.http:Constructing openlineage client to send events to http://localhost:5000
DEBUG:openlineage.client.transport.http:Sending openlineage event {"eventTime": "2022-12-07T02:15:58.090994+00:00", "eventType": "START", "inputs": [{"facets": {"schema": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SchemaDatasetFacet", "fields": [{"description": "the user id", "name": "id", "type": "BIGINT"}, {"description": "the user id", "name": "email_domain", "type": "VARCHAR"}, {"description": "the user id", "name": "status", "type": "BIGINT"}, {"description": "date and time of creation of the user", "name": "created_at", "type": "DATETIME"}, {"description": "the last time this row was updated", "name": "updated_at", "type": "DATETIME"}, {"description": "the time the data was fetched", "name": "fetch_time_utc", "type": "DATETIME"}, {"description": "the original file this data was ingested from", "name": "load_filename", "type": "VARCHAR"}, {"description": "the row number in the original file", "name": "load_filerow", "type": "INT"}, {"description": "the time the data was ingested", "name": "load_timestamp", "type": "DATETIME"}]}}, "name": "temp_demo.user_history", "namespace": "python_client"}], "job": {"facets": {"sourceCodeLocation": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SourceCodeLocationJobFacet", "type": "git", "url": "https://github.com/some/airflow/dags/example/user_trends.py"}, "sql": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SqlJobFacet", "query": "CREATE OR REPLACE TABLE TMP_DEMO.USER_COUNTS AS (\n\t\t\tSELECT DATE_TRUNC(DAY, created_at) date, COUNT(id) as user_count\n\t\t\tFROM TMP_DEMO.USER_HISTORY\n\t\t\tGROUP BY date\n\t\t\t)"}}, "name": "create_user_counts", "namespace": "python_client"}, "outputs": [{"facets": {}, "name": "tmp_demo.user_counts", "namespace": "python_client"}], "producer": "https://github.com/openlineage-user", "run": {"facets": {"nominalTime": {"_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python", "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet", "nominalStartTime": "2022-04-14T00:12:00Z"}}, "runId": "c321058c-276b-4d1a-a260-8e16f2137c2b"}}
DEBUG:urllib3.connectionpool:Starting new HTTP connection (1): localhost:5000
Traceback (most recent call last):
  File "/opt/homebrew/Caskroom/miniconda/base/envs/openlineage/lib/python3.9/site-packages/urllib3/connection.py", line 174, in _new_conn
    conn = connection.create_connection(
  File "/opt/homebrew/Caskroom/miniconda/base/envs/openlineage/lib/python3.9/site-packages/urllib3/util/connection.py", line 95, in create_connection
    raise err
  File "/opt/homebrew/Caskroom/miniconda/base/envs/openlineage/lib/python3.9/site-packages/urllib3/util/connection.py", line 85, in create_connection
    sock.connect(sa)
ConnectionRefusedError: [Errno 61] Connection refused
```

If you wish to output loggigng message to a file, you can modify the basic configuration as following:
```python
...
logging.basicConfig(filename='debug.log', encoding='utf-8', level=logging.DEBUG)
...
```

And the output will be saved to a file `debug.log`.

### Further readings
- https://docs.python.org/3/library/logging.html
- https://realpython.com/python-logging/
