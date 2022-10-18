from openlineage.airflow.extractors.postgres_extractor import PostgresExtractor
from openlineage.airflow.extractors.sql_check_extractors import get_check_extractors


COLUMN_CHECK_MAPPING = {
    "X": {
        "null_check": {
            "pass_value": 0,
            "tolerance": 0.0,
            "result": 0,
            "success": True
        },
        "distinct_check": {
            "pass_value": 5,
            "tolerance": 0.0,
            "result": 6,
            "success": False
        }
    }
}
TABLE_CHECK_MAPPING = {
    "row_count_check": {
        "pass_value": 9,
        "result": 9,
        "success": True
    }
}

(
    SqlCheckExtractor,
    SqlValueCheckExtractor,
    SqlThresholdCheckExtractor,
    SqlIntervalCheckExtractor,
    SqlColumnCheckExtractor,
    SqlTableCheckExtractor
) = get_check_extractors(PostgresExtractor)

class SQLTableCheckOperator:
    checks = TABLE_CHECK_MAPPING

class SQLColumnCheckOperator:
    column_mapping = COLUMN_CHECK_MAPPING


def test_get_table_input_facets():
    extractor = SqlTableCheckExtractor(SQLTableCheckOperator())
    facets = extractor._get_input_facets()
    data_quality_facet = facets["dataQuality"]
    assertions_facet = facets["dataQualityAssertions"]
    assert data_quality_facet.rowCount == 9
    assert data_quality_facet.bytes is None
    assert assertions_facet.assertions[0].assertion == "row_count_check"
    assert assertions_facet.assertions[0].success


def test_get_column_input_facets():
    extractor = SqlColumnCheckExtractor(SQLColumnCheckOperator())
    facets = extractor._get_input_facets()
    data_quality_facet = facets["dataQuality"]
    assertions_facet = facets["dataQualityAssertions"]
    assert data_quality_facet.columnMetrics.get("X").nullCount == 0
    assert data_quality_facet.columnMetrics.get("X").distinctCount == 6
    assert data_quality_facet.rowCount is None
    assert data_quality_facet.bytes is None
    assert assertions_facet.assertions[0].assertion == "null_check"
    assert assertions_facet.assertions[0].success
    assert assertions_facet.assertions[0].column == "X"
    assert assertions_facet.assertions[1].assertion == "distinct_check"
    assert not assertions_facet.assertions[1].success
    assert assertions_facet.assertions[1].column == "X"
