---
title: Setup a development environment
sidebar_position: 1
---

There are four Python OpenLineage packages that you can install locally when setting up a development environment.<br />
Two of them: [openlineage-integration-common](https://pypi.org/project/openlineage-integration-common/) and [openlineage-airflow](https://pypi.org/project/openlineage-airflow/) have dependecy on [openlineage-python](https://pypi.org/project/openlineage-python/) client and [openlineage-sql](https://pypi.org/project/openlineage-sql/).

Typically, you first need to build `openlineage-sql` locally (see [README](https://github.com/OpenLineage/OpenLineage/blob/main/integration/sql/README.md)). After each release you have to repeat this step in order to bump local version of the package.

To install Openlineage Common, Python Client & Dagster integration you need to run pip install command with a link to local directory:

```bash
$ python -m pip install -e .[dev]
```
In zsh:
```bash
$ python -m pip install -e .\[dev\]
```

To make Airflow integration setup easier you can use run following command in package directory:
```bash
$ pip install -r dev-requirements.txt
```
This should install all needed integrations locally.

### Docker Compose development environment
There is also possibility to create local Docker-based development environment that has OpenLineage libraries setup along with Airflow and some helpful services.
To do that you should run `run-dev-airflow.sh` script located [here](https://github.com/OpenLineage/OpenLineage/blob/main/integration/airflow/scripts/run-dev-airflow.sh).

The script uses the same Docker Compose files as [integration tests](./tests/airflow.md#integration-tests). Two main differences are:
* it runs in non-blocking way
* it mounts OpenLineage Python packages as editable and mounted to Airflow containers. This allows to change code and test it live without need to rebuild whole environment.


When using above script, you can add the `-i` flag or `--attach-integration` flag.
This can be helpful when you need to run arbitrary integration tests during development. For example, the following command run in the integration container...
```bash
python -m pytest test_integration.py::test_integration[great_expectations_validation-requests/great_expectations.json]
```
...runs a single test which you can repeat after changes in code.