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
import re
import subprocess

import pkg_resources
from setuptools import find_packages, setup


def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), 'r') as fp:
        return fp.read()


def execute_git(cwd, params):
    p = subprocess.Popen(['git'] + params,
                         cwd=cwd, stdout=subprocess.PIPE, stderr=None)
    p.wait(timeout=0.5)
    out, err = p.communicate()
    return out.decode('utf8').strip()


def get_version(rel_path):
    tag = execute_git(None, ['tag', '--points-at', 'HEAD'])

    if tag:
        if re.match(r'^[0-9]+(\.[0-9]+){2}(rc[0-9]+)?$', tag):
            return tag

    for line in read(rel_path).splitlines():
        if line.startswith('VERSION'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


with open("README.md", "r") as f:
    long_description = f.read()

NAME = "openlineage-python"

setup(
    name=NAME,
    python_requires='>=3.6',
    version=get_version('version.py'),
    author="OpenLineage Team",
    author_email="",
    description="OpenLineage python client",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/OpenLineage/OpenLineage",
    packages=find_packages(),
    install_requires=[
        "attrs>=20.3.0",
        "requests>=2.25.1"
    ]
)
