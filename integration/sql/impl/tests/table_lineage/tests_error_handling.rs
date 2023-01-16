use crate::test_utils::*;
use openlineage_sql::{
    ColumnLineage, ColumnMeta, DbTableMeta, ExtractionError, SqlMeta, TableLineage,
};

#[test]
fn test_failing_statement_with_insert() {
    assert_eq!(
        test_multiple_sql(vec![
            "FAILING STATEMENT;",
            "INSERT INTO Persons SELECT key FROM temp.table;",
            "INSERT ALSO FAILING"
        ])
        .unwrap(),
        SqlMeta {
            table_lineage: TableLineage {
                in_tables: tables(vec!["temp.table"]),
                out_tables: tables(vec!["Persons"])
            },
            column_lineage: vec![ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "key".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(DbTableMeta::new_default_dialect("temp.table".to_string())),
                    name: "key".to_string(),
                }],
            }],
            errors: vec![
                ExtractionError {
                    index: 0,
                    message: "Expected an SQL statement, found: FAILING".to_string(),
                    origin_statement: "FAILING STATEMENT;".to_string(),
                },
                ExtractionError {
                    index: 2,
                    message:
                        "Expected SELECT, VALUES, or a subquery in the query body, found: FAILING"
                            .to_string(),
                    origin_statement: "INSERT ALSO FAILING".to_string(),
                }
            ],
        }
    )
}
