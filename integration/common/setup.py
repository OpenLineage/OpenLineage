#!/usr/bin/env python
#
# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# -*- coding: utf-8 -*-

from setuptools import find_namespace_packages, setup

with open("README.md") as readme_file:
    readme = readme_file.read()

__version__ = "0.14.0"

requirements = [
    "attrs>=19.3.0",
    f"openlineage-python=={__version__}",
    "sqlparse>=0.3.1",
]

extras_require = {
    "sql": [
        f"openlineage_sql=={__version__}"
    ],
    "bigquery": [
        "google-api-core>=1.26.3",
        "google-auth>=1.30.0",
        "google-cloud-bigquery>=2.15.0,<3.0.0",
        "google-cloud-core>=1.6.0",
        "google-crc32c>=1.1.2"
    ],
    "dbt": [
        "dbt-core>=0.20.0",
        "pyyaml>=5.3.1"
    ],
    "great_expectations": [
        "great_expectations>=0.13.26,<=0.15.19",
        "sqlalchemy>=1.3.24"
    ],
    "redshift": [
        "boto3>=1.15.0"
    ],
    "tests": [
        "pytest",
        "pytest-cov",
        "mock",
        "flake8",
        "pandas",
        "jinja2",
        "python-dateutil",
        "mypy>=0.9.6",
        "types-python-dateutil",
        "types-PyYAML"
    ],
}
extras_require["dev"] = set(sum(extras_require.values(), []))
extras_require["dev_no_parser"] = set(
    sum({
        k: extras_require[k] for k in extras_require.keys() if k not in ["sql", "dev"]
    }.values(), [])
)

setup(
    name="openlineage-integration-common",
    version=__version__,
    description="OpenLineage common python library for integrations",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="OpenLineage",
    packages=find_namespace_packages(include=["openlineage.*"]),
    include_package_data=True,
    install_requires=requirements,
    extras_require=extras_require,
    python_requires=">=3.7",
    zip_safe=False,
    keywords="openlineage",
)
