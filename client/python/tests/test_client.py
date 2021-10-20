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

from unittest.mock import MagicMock

import pytest

from openlineage.client.client import OpenLineageClient
from openlineage.client.run import RunEvent, RunState, Run, Job


def test_client_fails_with_wrong_event_type():
    session = MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

    with pytest.raises(ValueError):
        client.emit("event")


def test_client_fails_to_create_with_wrong_url():
    session = MagicMock()
    with pytest.raises(ValueError):
        OpenLineageClient(url="notanurl", session=session)
    with pytest.raises(ValueError):
        OpenLineageClient(url="http://", session=session)
    with pytest.raises(ValueError):
        OpenLineageClient(url="example.com", session=session)
    with pytest.raises(ValueError):
        OpenLineageClient(url="http:example.com", session=session)
    with pytest.raises(ValueError):
        OpenLineageClient(url="http:/example.com", session=session)
    with pytest.raises(ValueError):
        OpenLineageClient(url="196.168.0.1", session=session)


def test_client_passes_to_create_with_valid_url():
    session = MagicMock()
    assert OpenLineageClient(url="http://196.168.0.1", session=session).url == \
           "http://196.168.0.1"
    assert OpenLineageClient(url="http://196.168.0.1", session=session).url == \
           "http://196.168.0.1"
    assert OpenLineageClient(url="http://example.com  ", session=session).url == \
           "http://example.com"
    assert OpenLineageClient(url=" http://example.com", session=session).url == \
           "http://example.com"
    assert OpenLineageClient(url="  http://marquez:5000  ", session=session).url == \
           "http://marquez:5000"
    assert OpenLineageClient(url="  https://marquez  ", session=session).url == \
           "https://marquez"


def test_client_sends_proper_json_with_minimal_event():
    session = MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

    client.emit(
        RunEvent(
            RunState.START,
            "2020-01-01",
            Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3"),
            Job("openlineage", "job"),
            "producer"
        )
    )

    session.post.assert_called_with(
        "http://example.com/api/v1/lineage",
        '{"eventTime": "2020-01-01", "eventType": "START", "inputs": [], "job": '
        '{"facets": {}, "name": "job", "namespace": "openlineage"}, "outputs": [], '
        '"producer": "producer", "run": {"facets": {}, "runId": '
        '"69f4acab-b87d-4fc0-b27b-8ea950370ff3"}}',
        timeout=5.0,
        verify=True
    )
