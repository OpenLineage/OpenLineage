#!/usr/bin/env python
#
# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from setuptools import find_namespace_packages, setup

with open("README.md") as readme_file:
    readme = readme_file.read()

__version__ = "1.36.0"

requirements = [
    "tqdm>=4.62.0",
    f"openlineage-integration-common[dbt]=={__version__}",
]


extras_require = {
    "tests": ["pytest", "pytest-cov", "mock", "ruff", "mypy>=0.9.6", "python-dateutil"],
}
extras_require["dev"] = set(sum(extras_require.values(), []))

setup(
    name="openlineage-dbt",
    version=__version__,
    description="OpenLineage integration with dbt",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="OpenLineage",
    packages=find_namespace_packages(include=["openlineage.*"]),
    entry_points={
        "console_scripts": ["dbt-ol = openlineage.dbt:main"],
    },
    include_package_data=True,
    install_requires=requirements,
    extras_require=extras_require,
    python_requires=">=3.9",
    zip_safe=False,
    keywords="openlineage",
)
