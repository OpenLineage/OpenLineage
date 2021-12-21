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
from pkg_resources import parse_version
from airflow.version import version as AIRFLOW_VERSION

__author__ = """OpenLineage"""
__version__ = "0.5.0"

if parse_version(AIRFLOW_VERSION) < parse_version("2.0.0"):
    from openlineage.airflow.dag import DAG
    __all__ = ["DAG"]
