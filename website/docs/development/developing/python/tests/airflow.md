---
title: Airflow
sidebar_position: 2
---

OpenLineage provides an integration with Apache Airflow. As Airflow is actively developed and major changes happen quite often it is advised to test OpenLineage integration against multiple Airflow versions. In the current CI process OpenLineage is tested against following versions:
* 2.5.2
* 2.6.1

### Unit tests
In order to make running unit tests against multiple Airflow versions easier there is possibility to use [tox](https://tox.wiki/).
To run unit tests against all configured Airflow versions just run:
```
tox
```
You can also list existing environments with:
```
tox -l
```
that should list:
```
py3-airflow-2.5.2
py3-airflow-2.6.1
```
Then you can run tests in chosen environment, e.g.:
```
tox -e py3-airflow-2.5.2
```
`setup.cfg` contains tox-related configuration. By default `tox` command runs:
1. `flake8` linting
2. `pytest` command

Additionally, outside of `tox` you should run `mypy` static code analysis. You can do that with:
```
python -m mypy openlineage
```

### Integration tests
Integration tests are located in `tests/integration/tests` directory. They require running Docker containers to provision local test environment: Airflow components (worker, scheduler), databases (PostgreSQL, MySQL) and OpenLineage events consumer.

#### How to run
Integration tests require usage of _docker compose_. There are scripts prepared to make build images and run tests easier.

```bash
AIRFLOW_IMAGE=<name-of-airflow-image> ./tests/integration/docker/up.sh
```
e.g.
```bash
AIRFLOW_IMAGE=apache/airflow:2.5.2-python3.9 ./tests/integration/docker/up.sh
```
#### What tests are ran
The actual setup is to run all defined Airflow DAGs, collect OpenLineage events and check if they meet requirements.
The test you should pay most attention to is `test_integration`. It compares produced events to expected JSON structures recursively, with a respect if fields are not missing.

Some of the tests are skipped if database connection specific environment variables are not set. The example is set of `SNOWFLAKE_PASSWORD` and `SNOWFLAKE_ACCOUNT_ID` variables.

#### View stored OpenLineage events
OpenLineage events produced from Airflow runs are stored locally in `./tests/integration/tests/events` directory. The files are not overwritten, rather new events are appended to existing files.

#### Example how to add new integration test
Let's take following `CustomOperator` for which we should add `CustomExtractor` and test it. First we create DAG in integration tests DAGs folder: [airflow/tests/integration/tests/airflow/dags](https://github.com/OpenLineage/OpenLineage/tree/main/integration/airflow/tests/integration/tests/airflow/dags).

```python
from airflow.models import BaseOperator
from airflow.utils.dates import days_ago
from airflow import DAG


default_args = {
    'depends_on_past': False,
    'start_date': days_ago(7)
}


dag = DAG(
    'custom_extractor',
    schedule_interval='@once',
    default_args=default_args
)

class CustomOperator(BaseOperator):
    def execute(self, context: Any):
        for i in range(10):
            print(i)

t1 = CustomOperator(
    task_id='custom_extractor',
    dag=dag
)
```
In the same folder we create `custom_extractor.py`:
```python
from typing import Union, Optional, List

from openlineage.client.run import Dataset
from openlineage.airflow.extractors import TaskMetadata
from openlineage.airflow.extractors.base import BaseExtractor


class CustomExtractor(BaseExtractor):
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['CustomOperator']

    def extract(self) -> Union[Optional[TaskMetadata], List[TaskMetadata]]:
        return TaskMetadata(
            "test",
            inputs=[
                Dataset(
                    namespace="test",
                    name="dataset",
                    facets={}
                )
            ]
        )
```
Typically we want to compare produced metadata against expected. In order to do that we create JSON file `custom_extractor.json` in [airflow/tests/integration/requests](https://github.com/OpenLineage/OpenLineage/tree/main/integration/airflow/tests/integration/requests):
```
 [{
 		"eventType": "START",
 		"inputs": [{
 			"facets": {},
 			"name": "dataset",
 			"namespace": "test"
 		}],
 		"job": {
 			"facets": {
 				"documentation": {
 					"description": "Test dag."
 				}
 			},
 			"name": "custom_extractor.custom_extractor",
 			"namespace": "food_delivery"
 		},
 		"run": {
 			"facets": {
 				"airflow_runArgs": {
 					"externalTrigger": false
 				},
 				"parent": {
 					"job": {
 						"name": "custom_extractor",
 						"namespace": "food_delivery"
 					}
 				}
 			}
 		}
 	},
 	{
 		"eventType": "COMPLETE",
 		"inputs": [{
 			"facets": {},
 			"name": "dataset",
 			"namespace": "test"
 		}],
 		"job": {
 			"facets": {},
 			"name": "custom_extractor.custom_extractor",
 			"namespace": "food_delivery"
 		}
 	}
 ]
 ```
 and add parameter for `test_integration` in [airflow/tests/integration/test_integration.py](https://github.com/OpenLineage/OpenLineage/blob/main/integration/airflow/tests/integration/test_integration.py):
```
("source_code_dag", "requests/source_code.json"),
+ ("custom_extractor", "requests/custom_extractor.json"),
("unknown_operator_dag", "requests/unknown_operator.json"),
```

That should setup a check for existence of both `START` and `COMPLETE` events, custom input facet and correct job facet.

Full example can be found in source code available in integration tests [directory](https://github.com/OpenLineage/OpenLineage/blob/main/integration/airflow/tests/integration/).