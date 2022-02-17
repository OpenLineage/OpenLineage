# SPDX-License-Identifier: Apache-2.0.
#
# -*- coding: utf-8 -*-
from pkg_resources import parse_version
from airflow.version import version as AIRFLOW_VERSION

__author__ = """OpenLineage"""
__version__ = "0.6.0"

if parse_version(AIRFLOW_VERSION) < parse_version("2.0.0"):
    from openlineage.airflow.dag import DAG
    __all__ = ["DAG"]
