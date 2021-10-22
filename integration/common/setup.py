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

from setuptools import find_namespace_packages, setup

with open("README.md") as readme_file:
    readme = readme_file.read()

__version__ = "0.3.1"

requirements = [
    "attrs>=19.3.0",
    f"openlineage-python=={__version__}",
    "sqlparse>=0.3.1",
]

extras_require = {
    "bigquery": [
        "google-api-core>=1.26.3",
        "google-auth>=1.30.0",
        "google-cloud-bigquery>=2.15.0",
        "google-cloud-core>=1.6.0",
        "google-crc32c>=1.1.2"
    ],
    "dbt": [
        "dbt-core>=0.20.0",
        "pyyaml>=5.3.1"
    ],
    "great_expectations": [
        "great_expectations==0.13.26",
        "sqlalchemy>=1.3.24"
    ],
    "tests": [
        "pytest",
        "pytest-cov",
        "mock",
        "flake8",
        "pandas",
        "jinja2",
        "python-dateutil"
    ],
}
extras_require["dev"] = set(sum(extras_require.values(), []))

setup(
    name="openlineage-integration-common",
    version=__version__,
    description="OpenLineage common python library for integrations",
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
