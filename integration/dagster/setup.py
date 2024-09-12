#!/usr/bin/env python
#
# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# -*- coding: utf-8 -*-
# import setuptools
from setuptools import find_namespace_packages, setup

with open("README.md") as readme_file:
    readme = readme_file.read()

__version__ = "1.23.0"

DAGSTER_VERSION = "1.0.0"

requirements = [
    "attrs>=19.3",
    "cattrs",
    "protobuf<=3.20.0",
    f"dagster>={DAGSTER_VERSION},<=1.6.9",
    f"openlineage-python=={__version__}",
]

extras_require = {
    "tests": [
        "pytest",
        "pytest-cov",
        "mypy>=0.9.6",
        # The relevant compat layer of pydantic v2 is shipped with Dagster `1.5.5`.
        # https://github.com/dagster-io/dagster/issues/15162
        "pydantic<2.0.0",
        "sqlalchemy<2.0.0",
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
    packages=find_namespace_packages(include=["openlineage.*"]),
    include_package_data=True,
    install_requires=requirements,
    extras_require=extras_require,
    python_requires=">=3.8",
    zip_safe=False,
    keywords="openlineage",
)
