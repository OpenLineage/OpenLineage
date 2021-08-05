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

from setuptools import setup, find_namespace_packages

__version__ = "0.0.1rc8"

with open("README.md", "r") as f:
    long_description = f.read()

NAME = "openlineage-python"

setup(
    name=NAME,
    python_requires='>=3.6',
    version=__version__,
    author="OpenLineage",
    author_email="",
    description="OpenLineage python client",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/OpenLineage/OpenLineage",
    packages=find_namespace_packages(include=['openlineage.*']),
    install_requires=[
        "attrs>=19.3.0",
        "requests>=2.20.0"
    ]
)
