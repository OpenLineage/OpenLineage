from openlineage.common.sql import parse, ColumnLineage, ColumnMeta, DbTableMeta


def test_column_level_lineage():
    cl = parse("""
        WITH cte1 AS (
            SELECT col1, col2
            FROM table1
            WHERE col1 = 'value1'
        ), cte2 AS (
            SELECT col3, col4
            FROM table2
            WHERE col2 = 'value2'
        )
        SELECT cte1.col1, cte2.col3
        FROM cte1
        JOIN cte2 ON cte1.col2 = cte2.col4
    """)

    assert cl.column_lineage == [
        ColumnLineage(
            descendant=ColumnMeta("col1"),
            lineage=[
                ColumnMeta("col1", DbTableMeta("table1")),
            ]
        ),
        ColumnLineage(
            descendant=ColumnMeta("col3"),
            lineage=[
                ColumnMeta("col3", DbTableMeta("table2")),
            ]
        )
    ]
    #
    # let y: SqlMeta = SqlMeta {
    #     in_tables: vec![pytable("table1"), pytable("table2")],
    #     out_tables: vec![],
    #     column_lineage: vec![ColumnLineage {
    #         descendant: pycolumn("col1"),
    #         lineage: vec![pycolumn_with_origin("col1", "table1"), ]
    #     }, ColumnLineage {
    #         descendant: pycolumn("col3"),
    #         lineage: vec![pycolumn_with_origin("col3", "table2")]
    #     }],
    # };
    # assert_eq!(x, y);
