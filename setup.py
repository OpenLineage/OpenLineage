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

import codecs
import os

from setuptools import find_packages, setup


def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), 'r') as fp:
        return fp.read()


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('VERSION'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


with open("README.md", "r") as f:
    long_description = f.read()

NAME = "marquez-airflow"

setup(
    name=NAME,
    version=get_version('marquez_airflow/version.py'),
    author="Marquez Team",
    author_email="",
    description="Marquez integration with Airflow",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/MarquezProject/marquez-airflow",
    packages=find_packages(),
    install_requires=[
        "marquez-python==0.12.0",
        "sqlparse==0.4.1"
    ]
)
