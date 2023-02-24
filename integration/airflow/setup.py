#!/usr/bin/env python
#
# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# -*- coding: utf-8 -*-

from setuptools import find_namespace_packages, setup

with open("README.md") as readme_file:
    readme = readme_file.read()

__version__ = "0.21.0"

requirements = [
    "attrs>=19.3",
    "requests>=2.20.0",
    f"openlineage-integration-common[sql]=={__version__}",
    f"openlineage-python=={__version__}",
]

extras_require = {
    "tests": [
        "pytest",
        "pytest-cov",
        "mock",
        "ruff",
        "SQLAlchemy",       # must be set to 1.3.* for airflow tests compatibility
        "Flask-SQLAlchemy",  # must be set to 2.4.* for airflow tests compatibility
        "pandas-gbq==0.14.1",       # must be set to 0.14.* for airflow tests compatibility
        "snowflake-connector-python",
    ],
    "airflow": [
        "apache-airflow-providers-postgres>=2.0.0",
        "apache-airflow-providers-mysql>=2.0.0",
        "apache-airflow-providers-trino>=3.1.0",
        "apache-airflow-providers-snowflake>=2.1.0",
        "apache-airflow-providers-google>=5.0.0",
        "apache-airflow-providers-amazon>=3.1.1",
        "apache-airflow-providers-sftp>=2.1.1",
        "apache-airflow-providers-ssh>=2.1.0",
        "apache-airflow-providers-ftp>=3.3.0",
        "airflow-provider-great-expectations==0.1.5",
        "great-expectations<=0.15.23",
        "protobuf>=3.20,<4.23",
    ],
}

extras_require["dev"] = extras_require["tests"]

setup(
    name="openlineage-airflow",
    version=__version__,
    description="OpenLineage integration with Airflow",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="OpenLineage",
    packages=find_namespace_packages(include=['openlineage.*']),
    include_package_data=True,
    install_requires=requirements,
    extras_require=extras_require,
    python_requires=">=3.7",
    zip_safe=False,
    keywords="openlineage",
    entry_points={
        "airflow.plugins": ["OpenLineagePlugin = openlineage.airflow.plugin:OpenLineagePlugin"]
    }
)
