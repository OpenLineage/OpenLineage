# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import socket
from ftplib import FTP_PORT
from typing import List, Optional

from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.airflow.utils import try_import_from_string
from openlineage.common.dataset import Dataset, Source

FTPOperation = try_import_from_string("airflow.providers.ftp.operators.ftp.FTPOperation")


class FTPExtractor(BaseExtractor):
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["FTPFileTransmitOperator"]

    def extract(self) -> Optional[TaskMetadata]:
        scheme = "file"

        local_host = socket.gethostname()
        try:
            local_host = socket.gethostbyname(local_host)
        except Exception as e:
            self.log.warning(
                f"Failed to resolve local hostname. Using the hostname got by socket.gethostbyname() without resolution. {e}",  # noqa: E501
                exc_info=True,
            )

        conn = self.operator.hook.get_conn()
        remote_host = conn.host
        remote_port = conn.port

        if isinstance(self.operator.local_filepath, str):
            local_filepath = [self.operator.local_filepath]
        else:
            local_filepath = self.operator.local_filepath
        if isinstance(self.operator.remote_filepath, str):
            remote_filepath = [self.operator.remote_filepath]
        else:
            remote_filepath = self.operator.remote_filepath

        local_datasets = [
            Dataset(source=self._get_source(scheme, local_host, None, path), name=path)
            for path in local_filepath
        ]
        remote_datasets = [
            Dataset(
                source=self._get_source(scheme, remote_host, remote_port, path),
                name=path,
            )
            for path in remote_filepath
        ]

        if self.operator.operation.lower() == FTPOperation.GET:
            inputs = remote_datasets
            outputs = local_datasets
        else:
            inputs = local_datasets
            outputs = remote_datasets

        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            inputs=[ds.to_openlineage_dataset() for ds in inputs],
            outputs=[ds.to_openlineage_dataset() for ds in outputs],
            run_facets={},
            job_facets={},
        )

    def _get_source(self, scheme, host, port, path) -> Source:
        port = port or FTP_PORT
        authority = f"{host}:{port}"
        return Source(
            scheme=scheme,
            authority=authority,
            connection_url=f"{scheme}://{authority}{path}",
        )
