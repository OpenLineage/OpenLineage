import attr
import prefect
from prefect import Task
from prefect._version import get_versions
from openlineage.client.facet import BaseFacet

from openlineage.prefect.util import task_qualified_name


@attr.s
class PrefectRunFacet(BaseFacet):
    task: str = attr.ib()
    prefect_version: str = attr.ib()
    prefect_commit: str = attr.ib()
    prefect_backend: str = attr.ib()
    openlineage_prefect_version: str = attr.ib()

    @classmethod
    def from_task(cls, task: Task):
        from openlineage.prefect.adapter import OPENLINEAGE_PREFECT_VERSION

        context = prefect.context
        version = get_versions()
        return cls(
            task=task_qualified_name(task),
            prefect_version=version["version"],
            prefect_commit=version["full-revisionid"],
            prefect_backend=context.config.backend,
            openlineage_prefect_version=OPENLINEAGE_PREFECT_VERSION,
        )
