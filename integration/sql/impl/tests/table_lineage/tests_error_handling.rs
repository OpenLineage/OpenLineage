// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::*;
use openlineage_sql::{ColumnLineage, ColumnMeta, ExtractionError, SqlMeta, TableLineage};

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
                    origin: Some(table("temp.table")),
                    name: "key".to_string(),
                }],
            }],
            errors: vec![
                ExtractionError {
                    index: 0,
                    message: "Expected: an SQL statement, found: FAILING at Line: 1, Column: 1"
                        .to_string(),
                    origin_statement: "FAILING STATEMENT;".to_string(),
                },
                ExtractionError {
                    index: 2,
                    message:
                        "Expected: SELECT, VALUES, or a subquery in the query body, found: EOF"
                            .to_string(),
                    origin_statement: "INSERT ALSO FAILING".to_string(),
                }
            ],
        }
    )
}

#[test]
fn test_failing_statement_tokenizer_failes() {
    let errors = test_multiple_sql(vec!["$$$"]).unwrap().errors;
    assert_eq!(
        errors,
        vec![ExtractionError {
            index: 0,
            message: "Unterminated dollar-quoted string at Line: 1, Column: 4".to_string(),
            origin_statement: "$$$".to_string()
        },],
    )
}

#[test]
fn test_unsupported_pipe_queries() {
    for dialect in ["databricks", "bigquery", "generic"] {
        for sql in [
            "FROM orders |> JOIN customers ON orders.customer_id = customers.id |> SELECT orders.id",
            "SELECT * FROM (FROM orders |> SELECT id) AS piped",
            "CREATE TABLE result AS FROM orders |> SELECT id",
        ] {
            let error = test_sql_dialect(sql, dialect).unwrap_err();
            assert_eq!(error.to_string(), "SQL pipe operators are not supported");
        }
    }
}

#[test]
fn test_unsupported_select_into_target() {
    let error = test_sql("SELECT a INTO (1 + 2) FROM source").unwrap_err();
    assert_eq!(
        error.to_string(),
        "SELECT INTO target not implemented: (1 + 2)"
    );
}
