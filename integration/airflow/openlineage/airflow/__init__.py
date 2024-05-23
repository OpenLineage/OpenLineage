# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

#
# -*- coding: utf-8 -*-
import logging

from openlineage.airflow.version import __version__
from packaging.version import Version

from airflow.version import version as AIRFLOW_VERSION

__author__ = """OpenLineage"""

if Version(AIRFLOW_VERSION) < Version("2.3.0"):  # type: ignore
    logging.warning(
        f"""
        OpenLineage support for Airflow version {AIRFLOW_VERSION} is REMOVED.
        Please make sure to upgrade your Airflow version to minimum of 2.3.0
        in order to continue using OpenLineage.
        OpenLineage version 1.14.0 is the last one that supported Airflow 2.1.x and 2.2.x
        """
    )
elif Version(AIRFLOW_VERSION) >= Version("2.8.0b1"):  # type: ignore
    logging.warning(
        f"""
        OpenLineage support for Airflow version {AIRFLOW_VERSION} is REMOVED.
        For Airflow 2.7 and later, use the native Airflow Openlineage provider package.
        Documentation can be found at https://airflow.apache.org/docs/apache-airflow-providers-openlineage
        """
    )

from airflow.models import DAG

__all__ = ["DAG", "__version__"]
