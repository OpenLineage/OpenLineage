#!/usr/bin/env python
#
# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# -*- coding: utf-8 -*-

from setuptools import find_namespace_packages, setup

with open("README.md") as readme_file:
    readme = readme_file.read()

requirements = [
    "attrs>=19.3.0",
    "requests>=2.20.0",
    "pyyaml>=5.1.0",
    "python-dateutil"
]

extras_require = {
    "tests": [
        "pytest",
        "pytest-cov",
        "mock",
        "ruff",
        "requests",
        "pyyaml",
        "mypy>=0.9.6",
        "types-requests",
        "types-PyYAML"
    ],
    "kafka": ["confluent-kafka"],
}
extras_require["dev"] = set(sum(extras_require.values(), []))

setup(
    name="openlineage-python",
    version="0.21.0",
    description="OpenLineage Python Client",
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
