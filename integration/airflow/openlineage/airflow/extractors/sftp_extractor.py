# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import socket
from typing import List, Optional

import paramiko
from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.common.dataset import Dataset, Source

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.operators.sftp import SFTPOperation


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
                exc_info=True,
            )

        # As of version 4.1.0, SFTPOperator accepts either of SFTPHook, SSHHook, or ssh_conn_id
        # as a constructor parameter. In the last case, SFTPOperator creates hook object in the
        # `execute` method rather than its constructor for some reason. So we have to manually
        # create a hook in the same way so that we can access connection information via that.
        if hasattr(self.operator, "sftp_hook") and self.operator.sftp_hook is not None:
            hook = self.operator.sftp_hook
        elif hasattr(self.operator, "ssh_hook") and self.operator.ssh_hook is not None:
            hook = self.operator.ssh_hook
        else:
            hook = SFTPHook(ssh_conn_id=self.operator.ssh_conn_id)

        if hasattr(self.operator, "remote_host") and self.operator.remote_host is not None:
            remote_host = self.operator.remote_host
        else:
            remote_host = hook.get_connection(hook.ssh_conn_id).host
        try:
            remote_host = socket.gethostbyname(remote_host)
        except Exception as e:
            self.log.warning(
                f"Failed to resolve remote hostname. Using the provided hostname without resolution. {e}",
                exc_info=True,
            )

        if hasattr(hook, "port"):
            remote_port = hook.port
        elif hasattr(hook, "ssh_hook"):
            remote_port = hook.ssh_hook.port

        # Since v4.1.0, SFTPOperator accepts both a string (single file) and a list of
        # strings (multiple files) as local_filepath and remote_filepath, and internally
        # keeps them as list in both cases. But before 4.1.0, only single string is
        # allowed. So we consider both cases here for backward compatibility.
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
        port = port or paramiko.config.SSH_PORT
        authority = f"{host}:{port}"
        return Source(
            scheme=scheme,
            authority=authority,
            connection_url=f"{scheme}://{authority}{path}",
        )
