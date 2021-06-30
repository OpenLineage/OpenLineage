import json
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch
import dbt.flags as flags
import dbt.exceptions
from dbt.adapters.base.query_headers import MacroQueryStringSetter
from dbt.adapters.openlineage_snowflake import Plugin as OpenLineagePlugin
from dbt.adapters.openlineage_snowflake.connections import PRODUCER
from openlineage.facet import SourceCodeLocationJobFacet, SqlJobFacet
from openlineage.run import RunEvent, RunState, Job, Run, Dataset

from dbt.adapters.openlineage_snowflake import OpenLineageSnowflakeAdapter
from .utils import config_from_parts_or_dicts, inject_adapter


def _openlineage_conn():
    conn = MagicMock()
    conn.get.side_effect = lambda x: 'openlineage' if x == 'type' else None
    return conn


class BaseTestOpenLineageSnowflakeAdapter(unittest.TestCase):
    def setUp(self):
        flags.STRICT_MODE = True

        self.raw_profile = {
            'outputs': {
                'test': {
                    'type': 'snowflake',
                    "account": "dummy-account",
                    "client_session_keep_alive": False,
                    "database": "DEMO_DB",
                    "openlineage_timeout": 5.0,
                    "openlineage_url": "https://localhost:8000",
                    "password": "dummy-password",
                    "role": "sysadmin",
                    "schema": "public",
                    "user": "user",
                    "warehouse": "COMPUTE_WH"
                }
            },
            'target': 'test',
        }

        self.project_cfg = {
            'name': 'X',
            'version': '0.1',
            'project-root': '/tmp/dbt/does-not-exist',
            'profile': 'default',
            'config-version': 2,
        }
        self.qh_patch = None

    def tearDown(self):
        if self.qh_patch:
            self.qh_patch.stop()
        super().tearDown()

    def get_adapter(self, target):
        project = self.project_cfg.copy()
        profile = self.raw_profile.copy()
        profile['target'] = target

        config = config_from_parts_or_dicts(
            project=project,
            profile=profile,
        )
        adapter = OpenLineageSnowflakeAdapter(config)

        adapter.connections.query_header = MacroQueryStringSetter(config, MagicMock(macros={}))

        self.qh_patch = patch.object(adapter.connections.query_header, 'add')
        self.mock_query_header_add = self.qh_patch.start()
        self.mock_query_header_add.side_effect = lambda q: '/* dbt */\n{}'.format(q)

        inject_adapter(adapter, OpenLineagePlugin)
        return adapter


class TestOpenLineageSnowflakeAdapterAcquire(BaseTestOpenLineageSnowflakeAdapter):
    @patch('dbt.adapters.openlineage_snowflake.OpenLineageSnowflakeConnectionManager.open',
           return_value=_openlineage_conn())
    def test_acquire_connection_test_validations(self, mock_open_connection):
        adapter = self.get_adapter('test')
        try:
            connection = adapter.acquire_connection('dummy')
            self.assertEqual(connection.type, 'openlineage_snowflake')

        except dbt.exceptions.ValidationException as e:
            self.fail('got ValidationException: {}'.format(str(e)))

        except BaseException:
            raise

        mock_open_connection.assert_not_called()
        connection.handle
        mock_open_connection.assert_called_once()

    @patch('uuid.uuid4')
    @patch('dbt.adapters.openlineage_snowflake.OpenLineageSnowflakeConnectionManager.get_openlineage_client')  # noqa
    def test_run(self, get_openlineage_client, uuid4):
        uuid4.return_value = '86aea653-25fa-4712-962f-6cb9c44e6317'
        client = MagicMock()
        get_openlineage_client.return_value = client
        run_started_now = datetime.now().isoformat()

        adapter = self.get_adapter('test')
        try:
            connection = adapter.acquire_connection('dummy')
            self.assertEqual(connection.type, 'openlineage_snowflake')

        except dbt.exceptions.ValidationException as e:
            self.fail('got ValidationException: {}'.format(str(e)))

        except BaseException:
            raise

        with open('tests/model_snowflake.json') as f:
            model = json.load(f)
            adapter.emit_start(model, run_started_now)

        get_openlineage_client.assert_called()
        client.emit.assert_called_with(RunEvent(
            eventType=RunState.START,
            eventTime=run_started_now,
            run=Run(runId='86aea653-25fa-4712-962f-6cb9c44e6317'),
            job=Job(
                namespace="dbt_snowflake_test",
                name="model.dbt_snowflake_test.test_third_dbt_model",
                facets={
                    "sourceCodeLocation": SourceCodeLocationJobFacet(
                        type="",
                        url="models/example/test_third_dbt_model.sql"
                    ),
                    "sql": SqlJobFacet(
                        query="select first.id as id, second.id as second_id from "
                              "DEMO_DB.public.test_second_dbt_model as first join "
                              "DEMO_DB.public.test_second_parallel_dbt_model "
                              "as second on first.id = second.id"
                    )
                }
            ),
            producer=PRODUCER,
            inputs=[
                Dataset(
                    namespace='snowflake://dummy-account',
                    name='DEMO_DB.public.test_second_dbt_model'
                ),
                Dataset(
                    namespace='snowflake://dummy-account',
                    name='DEMO_DB.public.test_second_parallel_dbt_model'
                )
            ],
            outputs=[
                Dataset(
                    namespace='snowflake://dummy-account',
                    name='DEMO_DB.public.test_third_dbt_model'
                )
            ]
        ))
