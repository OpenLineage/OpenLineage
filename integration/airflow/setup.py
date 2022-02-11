#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -*- coding: utf-8 -*-

from setuptools import setup, find_namespace_packages

with open("README.md") as readme_file:
    readme = readme_file.read()

__version__ = "0.5.2"

requirements = [
    "attrs>=19.3",
    "requests>=2.20.0",
    "sqlparse>=0.3.1",
    f"openlineage-integration-common=={__version__}",
    f"openlineage-python=={__version__}",
]

extras_require = {
    "tests": [
        "pytest",
        "pytest-cov",
        "mock",
        "flake8",
        "SQLAlchemy",       # must be set to 1.3.* for airflow tests compatibility
        "Flask-SQLAlchemy",  # must be set to 2.4.* for airflow tests compatibility
        "pandas-gbq==0.14.1",       # must be set to 0.14.* for airflow tests compatibility
        "snowflake-connector-python"
    ],
    "airflow-1": [
        "apache-airflow[gcp_api,google,postgres]==1.10.15",
        "airflow-provider-great-expectations==0.0.8",
    ],
    "airflow-2": [
        "apache-airflow==2.1.3",
        "apache-airflow-providers-postgres==2.0.0",
        "apache-airflow-providers-snowflake==2.1.0",
        "apache-airflow-providers-google==5.0.0",
        "airflow-provider-great-expectations==0.0.8",
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
    python_requires=">=3.6",
    zip_safe=False,
    keywords="openlineage",
)
