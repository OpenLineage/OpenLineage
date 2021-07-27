import os

from marquez_airflow.marquez import MarquezAdapter


def test_create_client_from_marquez_url():
    os.environ['MARQUEZ_URL'] = "http://marquez:5000"
    os.environ['MARQUEZ_API_KEY'] = "test-api-key"

    client = MarquezAdapter().get_or_create_openlineage_client()
    assert client.url == "http://marquez:5000"
    assert client.options.api_key == "test-api-key"
    del os.environ['MARQUEZ_URL']
    del os.environ['MARQUEZ_API_KEY']


def test_create_client_from_ol_env():
    os.environ['OPENLINEAGE_URL'] = "http://ol-api:5000"
    os.environ['OPENLINEAGE_API_KEY'] = "api-key"

    client = MarquezAdapter().get_or_create_openlineage_client()

    assert client.url == "http://ol-api:5000"
    assert client.options.api_key == "api-key"
    del os.environ['OPENLINEAGE_URL']
    del os.environ['OPENLINEAGE_API_KEY']
