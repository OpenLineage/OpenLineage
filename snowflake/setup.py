#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup

package_name = "openlineage-dbt-snowflake"
package_version = "0.15.2"
description = """The openlineage snowflake adapter plugin for dbt (data build tool)"""

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
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author='Marquez Project',
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/openlineage_snowflake/macros/*.sql',
            'include/openlineage_snowflake/dbt_project.yml',
        ]
    },
    install_requires=[
        "dbt-core>=0.20.0b1",
        "dbt-snowflake>=0.20.0b1",
        "sqlparse>=0.3.1",
        "openlineage-python>=0.0.1rc6"
    ],
    extras_require=extras_require,
    python_requires=">=3.6",
)
