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

__version__ = "0.0.1"

data = {
    'dbt': [
        'include/openlineage_snowflake/macros/*.sql',
        'include/openlineage_snowflake/macros/materializations/*.sql',
        'include/openlineage_snowflake/dbt_project.yml',
    ]
}

requirements = [
    "dbt-core>=0.20.0",
    "dbt-snowflake>=0.20.0",
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
    ],
}
extras_require["dev"] = set(sum(extras_require.values(), []))


setup(
    name="openlineage-dbt-snowflake",
    version=__version__,
    description="The OpenLineage Snowflake adapter plugin for dbt (data build tool)",
    long_description=readme,
    long_description_content_type="text/markdown",
    author='OpenLineage',
    packages=find_namespace_packages(include=['dbt', 'dbt.*', 'openlineage.*']),
    package_data=data,
    include_package_data=True,
    install_requires=requirements,
    extras_require=extras_require,
    python_requires=">=3.6",
    zip_safe=False,
    keywords="openlineage"
)
