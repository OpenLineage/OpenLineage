# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import socket
from typing import Optional, List

from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.common.dataset import Source, Dataset
from airflow.providers.sftp.operators.sftp import SFTPOperation
from airflow.providers.ssh.hooks.ssh import SSHHook


class SFTPExtractor(BaseExtractor):

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["SFTPOperator"]

    def extract(self) -> Optional[TaskMetadata]:
        scheme = "file"

        local_host = socket.gethostname()
        try:
            local_host = socket.gethostbyname(local_host)
        except Exception as e:
            self.log.warning(
                f"Failed to resolve local hostname. Using the hostname got by socket.gethostbyname() without resolution. {e}",  # noqa: E501
                exc_info=True
            )

        if hasattr(self.operator, "sftp_hook") and self.operator.sftp_hook is not None:
            hook = self.operator.sftp_hook
        elif hasattr(self.operator, "ssh_hook") and self.operator.ssh_hook is not None:
            hook = self.operator.ssh_hook
        else:
            hook = SSHHook(ssh_conn_id=self.operator.ssh_conn_id)

        if hasattr(self.operator, "remote_host") and self.operator.remote_host is not None:
            remote_host = self.operator.remote_host
        else:
            remote_host = hook.get_connection(hook.ssh_conn_id).host
        try:
            remote_host = socket.gethostbyname(remote_host)
        except Exception as e:
            self.log.warning(
                f"Failed to resolve remote hostname. Using the provided hostname without resolution. {e}",  # noqa: E501
                exc_info=True
            )

        local_datasets = [
            Dataset(source=self._get_source(scheme, local_host, None, path), name=path)
            for path in self.operator.local_filepath
        ]
        remote_datasets = [
            Dataset(source=self._get_source(
                scheme,
                remote_host,
                hook.port if hasattr(hook, "port") else hook.ssh_hook.port,
                path,
            ), name=path) for path in self.operator.remote_filepath
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

    def _get_source(self, scheme, host, port, path) -> Source:
        authority = host if port is None else f"{host}:{port}"
        return Source(
            scheme=scheme,
            authority=authority,
            connection_url=f"{scheme}://{authority}{path}"
        )
