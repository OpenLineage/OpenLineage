#!/usr/bin/env python
#
# SPDX-License-Identifier: Apache-2.0.
#
# -*- coding: utf-8 -*-

from setuptools import setup, find_namespace_packages

with open("README.md") as readme_file:
    readme = readme_file.read()

requirements = [
    "attrs>=19.3.0",
    "requests>=2.20.0",
]

extras_require = {
    "tests": ["pytest", "pytest-cov", "mock", "flake8", "requests"],
}
extras_require["dev"] = set(sum(extras_require.values(), []))

setup(
    name="openlineage-python",
    version="0.6.1",
    description="OpenLineage Python Client",
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
