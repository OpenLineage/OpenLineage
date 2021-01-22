#!/bin/bash
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
# Usage: $ ./release.py <type>

set -e

# Verify bump2version is installed
if [[ ! $(type -P bump2version) ]]; then
 echo "bump2version not installed! Please see https://github.com/c4urself/bump2version#installation"
 exit 1
fi

branch=$(git symbolic-ref --short HEAD)
if [[ "${branch}" != "main" ]]; then
  echo "Error: You may only release on 'main'!"
  exit 1;
fi

type=${1}
if [[ -z "${type}" ]]; then
  # Default to 'patch'
  type="patch"
fi

# Bump version
VERSION=$(python ./setup.py --version)
bump2version \
  --current-version "${VERSION}" \
  --commit \
  --tag \
  --tag-name {new_version} \
  "${type}" ./marquez_airflow/version.py

# Push tag
git push --tags origin main

echo "DONE!"