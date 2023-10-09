#!/usr/bin/env python
#
# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# -*- coding: utf-8 -*-

from setuptools import setup

with open("README.md") as readme_file:
     readme = readme_file.read()

__version__ = "1.4.1"

requirements = [
    "tqdm>=4.62.0",
    f"openlineage-integration-common[dbt]=={__version__}",
]


extras_require = {
    "tests": [
        "pytest",
        "pytest-cov",
        "mock",
        "ruff"
        "mypy>=0.9.6",
        "python-dateutil"
    ],
}
extras_require["dev"] = set(sum(extras_require.values(), []))

setup(
    name="openlineage-dbt",
    version=__version__,
    description="OpenLineage integration with dbt",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="OpenLineage",
    scripts=['scripts/dbt-ol'],
    include_package_data=True,
    install_requires=requirements,
    extras_require=extras_require,
    python_requires=">=3.8",
    zip_safe=False,
    keywords="openlineage",
)
