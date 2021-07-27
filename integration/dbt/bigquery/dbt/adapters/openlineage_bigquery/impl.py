from dbt.adapters.base import available
from dbt.adapters.bigquery import BigQueryAdapter
from dbt.adapters.openlineage_bigquery import OpenLineageBigQueryConnectionManager


class OpenLineageBigQueryAdapter(BigQueryAdapter):
    ConnectionManager = OpenLineageBigQueryConnectionManager

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sqlmap = {}

    @classmethod
    def type(cls) -> str:
        return 'openlineage_bigquery'

    @available.parse(lambda *a, **k: '')
    def emit_start(self, model, run_started_at):
        return self.connections.emit_start(model, run_started_at)

    @available.parse(lambda *a, **k: '')
    def emit_complete(self, run_id):
        return self.connections.emit_complete(run_id)
