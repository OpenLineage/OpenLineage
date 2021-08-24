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
    version="0.2.0",
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
