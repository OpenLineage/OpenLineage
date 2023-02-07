#!/usr/bin/env python
#
# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# -*- coding: utf-8 -*-
# import setuptools
from setuptools import find_namespace_packages, setup

with open("README.md") as readme_file:
    readme = readme_file.read()

__version__ = "0.21.0"

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
        "ruff",
        "mypy>=0.9.6",
        "sqlalchemy<2.0.0"
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
