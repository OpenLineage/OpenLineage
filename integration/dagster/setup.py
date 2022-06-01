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
# import setuptools
from setuptools import setup, find_namespace_packages

with open("README.md") as readme_file:
    readme = readme_file.read()

__version__ = "0.9.0"

DAGSTER_VERSION = "0.13.8"

requirements = [
    "attrs>=19.3",
    "cattrs",
    "protobuf<=3.20.0",
    f"dagster>={DAGSTER_VERSION},<=0.14.5",
    f"openlineage-python=={__version__}",
]

extras_require = {
    "tests": [
        "pytest",
        "pytest-cov",
        "flake8",
        "mypy>=0.9.6"
    ],
}

extras_require["dev"] = extras_require["tests"]

setup(
    name="openlineage-dagster",
    version=__version__,
    description="OpenLineage integration with Dagster",
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
)
