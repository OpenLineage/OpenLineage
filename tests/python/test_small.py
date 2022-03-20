from openlineage_sql import parse, QueryMetadata

def test_parse_small():
    metadata = parse("SELECT * FROM test1")
    assert metadata == QueryMetadata(inputs=["test1"], output=None)
