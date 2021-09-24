from unittest.mock import patch, MagicMock

import pytest


@pytest.fixture()
@patch("openlineage.prefect.adapter.OpenLineageClient")
@patch("openlineage.client.client.OpenLineageClient")
def mock_open_lineage_client(client, client2):
    # client.session = MagicMock()
    # client2.session = MagicMock()
    return client
