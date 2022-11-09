# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import socket
from typing import Optional, List

from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.common.dataset import Source, Dataset
from airflow.providers.sftp.operators.sftp import SFTPOperation
from airflow.providers.ssh.hooks.ssh import SSHHook


class SFTPExtractor(BaseExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["SFTPOperator"]

    def extract(self) -> Optional[TaskMetadata]:
        scheme = "file"
        local_host = socket.gethostname()

        if hasattr(self.operator, "remote_host") and self.operator.remote_host is not None:
            remote_host = self.operator.remote_host
        elif hasattr(self.operator, "sftp_hook") and self.operator.sftp_hook is not None:
            remote_host = self.operator.sftp_hook.get_connection(self.sftp_hook.ssh_conn_id).host
        elif hasattr(self.operator, "ssh_hook") and self.operator.ssh_hook is not None:
            remote_host = self.operator.ssh_hook.get_connection(self.ssh_hook.ssh_conn_id).host
        else:
            hook = SSHHook(ssh_conn_id=self.operator.ssh_conn_id)
            remote_host = hook.get_connection(self.operator.ssh_conn_id).host

        if isinstance(self.operator.local_filepath, str):
            local_filepath = [self.operator.local_filepath]
        else:
            local_filepath = self.operator.local_filepath

        if isinstance(self.operator.remote_filepath, str):
            remote_filepath = [self.operator.remote_filepath]
        else:
            remote_filepath = self.operator.remote_filepath

        local_datasets = [
            Dataset(source=self._get_source(scheme, local_host, path), name=path)
            for path in local_filepath
        ]
        remote_datasets = [
            Dataset(source=self._get_source(scheme, remote_host, path), name=path)
            for path in remote_filepath
        ]

        if self.operator.operation.lower() == SFTPOperation.GET:
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

    def _get_source(self, scheme, authority, path) -> Source:
        return Source(
            scheme=scheme,
            authority=authority,
            connection_url=f"{scheme}://{authority}{path}"
        )
