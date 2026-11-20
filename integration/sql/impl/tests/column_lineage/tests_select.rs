// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::{table, test_sql, test_sql_dialect};
use openlineage_sql::{ColumnLineage, ColumnMeta};

#[test]
fn test_multiple_derived_tables() {
    let output = test_sql(
        "SELECT t1.a, t2.b, t3.c FROM 
         (SELECT a, t.b FROM table1 t) t1,
         (SELECT t.b, t.c FROM table2 t) t2,
         (SELECT c, t.a FROM table3 t) t3",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "a".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("table1")),
                    name: "a".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "b".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("table2")),
                    name: "b".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "c".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("table3")),
                    name: "c".to_string()
                }]
            },
        ]
    );
}

#[test]
fn test_deeply_nested_selects() {
    let output = test_sql(
        "SELECT a FROM (
            SELECT a FROM (
                SELECT a FROM (
                    SELECT a FROM t
                )
            )
         )",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![ColumnLineage {
            descendant: ColumnMeta {
                origin: None,
                name: "a".to_string()
            },
            lineage: vec![ColumnMeta {
                origin: Some(table("t")),
                name: "a".to_string()
            }]
        },]
    );
}

#[test]
fn test_bigquery_unnest_struct_alias() {
    for dialect in ["bigquery", "generic"] {
        let output = test_sql_dialect(
            "SELECT e.field FROM p.d.t AS t, UNNEST(t.col) AS e",
            dialect,
        )
        .unwrap();
        assert!(output.errors.is_empty());
        assert_eq!(output.table_lineage.in_tables, vec![table("p.d.t")]);
        assert_eq!(
            output.column_lineage,
            vec![ColumnLineage {
                descendant: ColumnMeta::new("field".to_string(), None),
                lineage: vec![ColumnMeta::new("col".to_string(), Some(table("p.d.t")))],
            }],
            "{dialect}"
        );
    }
}

#[test]
fn test_bigquery_unnest_literal_alias() {
    for dialect in ["bigquery", "generic"] {
        let output = test_sql_dialect(
            "SELECT t.amount, currency FROM p.d.t AS t
             CROSS JOIN UNNEST(['USD', 'GBP']) AS currency",
            dialect,
        )
        .unwrap();
        assert!(output.errors.is_empty());
        assert_eq!(output.table_lineage.in_tables, vec![table("p.d.t")]);
        assert_eq!(
            output.column_lineage,
            vec![
                ColumnLineage {
                    descendant: ColumnMeta::new("amount".to_string(), None),
                    lineage: vec![ColumnMeta::new("amount".to_string(), Some(table("p.d.t")))],
                },
                ColumnLineage {
                    descendant: ColumnMeta::new("currency".to_string(), None),
                    lineage: vec![],
                },
            ],
            "{dialect}"
        );
    }
}

#[test]
fn test_bigquery_unnest_correlated_scalar_subquery() {
    for dialect in ["bigquery", "generic"] {
        let output = test_sql_dialect(
            "SELECT
              (SELECT LOGICAL_AND(MOD(b, 2) = 0 AND b BETWEEN 64 AND 252)
               FROM UNNEST(TO_CODE_POINTS(SAFE.FROM_BASE64(encoded_value))) AS b
              ) AS is_encoded
             FROM p.d.t",
            dialect,
        )
        .unwrap();
        assert!(output.errors.is_empty());
        assert_eq!(output.table_lineage.in_tables, vec![table("p.d.t")]);
        assert_eq!(output.column_lineage.len(), 1);
        assert_eq!(
            output.column_lineage[0].lineage,
            vec![ColumnMeta::new(
                "encoded_value".to_string(),
                Some(table("p.d.t"))
            )],
            "{dialect}"
        );
    }
}

#[test]
fn test_bigquery_unnest_array_sources() {
    for sql in [
        "SELECT e AS value FROM p.d.t AS t CROSS JOIN UNNEST(t.col) AS e",
        "SELECT E.field AS value FROM p.d.t AS t, UNNEST(t.col) AS e",
        "SELECT `E`.field AS value FROM p.d.t AS t, UNNEST(t.col) AS `e`",
        "SELECT e.field.child AS value FROM p.d.t AS t, UNNEST(t.col) AS e",
        "SELECT child.field AS value FROM p.d.t AS t,
         UNNEST(t.col) AS e, UNNEST(e.children) AS child",
        "SELECT e AS value FROM (SELECT col AS items FROM p.d.t) AS s
         CROSS JOIN UNNEST(s.items) AS e",
        "WITH c AS (SELECT col FROM p.d.t)
         SELECT e.field AS value FROM c CROSS JOIN UNNEST(c.col) AS e",
        "WITH c AS (SELECT col AS items FROM p.d.t)
         SELECT e AS value FROM c AS s CROSS JOIN UNNEST(s.items) AS e",
        "SELECT e AS value FROM UNNEST(ARRAY(SELECT col FROM p.d.t)) AS e",
    ] {
        let output = test_sql_dialect(sql, "bigquery").unwrap();
        assert!(output.errors.is_empty(), "{sql}");
        assert_eq!(
            output.table_lineage.in_tables,
            vec![table("p.d.t")],
            "{sql}"
        );
        assert_eq!(
            output.column_lineage,
            vec![ColumnLineage {
                descendant: ColumnMeta::new("value".to_string(), None),
                lineage: vec![ColumnMeta::new("col".to_string(), Some(table("p.d.t")))],
            }],
            "{sql}"
        );
    }
}

#[test]
fn test_bigquery_unnest_multiple_array_sources() {
    let output = test_sql_dialect(
        "SELECT e AS value FROM p.d.t AS t CROSS JOIN p.d.u AS u
         CROSS JOIN UNNEST(ARRAY_CONCAT(t.col, u.other)) AS e",
        "bigquery",
    )
    .unwrap();
    assert!(output.errors.is_empty());
    assert_eq!(
        output.table_lineage.in_tables,
        vec![table("p.d.t"), table("p.d.u")]
    );
    assert_eq!(
        output.column_lineage,
        vec![ColumnLineage {
            descendant: ColumnMeta::new("value".to_string(), None),
            lineage: vec![
                ColumnMeta::new("col".to_string(), Some(table("p.d.t"))),
                ColumnMeta::new("other".to_string(), Some(table("p.d.u"))),
            ],
        }]
    );
}

#[test]
fn test_bigquery_unnest_generated_columns() {
    for sql in [
        "SELECT n AS value FROM p.d.t CROSS JOIN UNNEST(GENERATE_ARRAY(1, 3)) AS n",
        "SELECT e.field AS value FROM p.d.t
         CROSS JOIN UNNEST([STRUCT(1 AS field), STRUCT(2 AS field)]) AS e",
        "SELECT position AS value FROM p.d.t
         CROSS JOIN UNNEST([1, 2]) AS e WITH OFFSET AS position",
        "SELECT offset AS value FROM p.d.t CROSS JOIN UNNEST([1, 2]) AS e WITH OFFSET",
        "WITH c AS (SELECT currency FROM p.d.t CROSS JOIN UNNEST(['USD']) AS currency)
         SELECT currency AS value FROM c",
        "SELECT s.currency AS value FROM
         (SELECT currency FROM p.d.t CROSS JOIN UNNEST(['USD']) AS currency) AS s",
    ] {
        let output = test_sql_dialect(sql, "bigquery").unwrap();
        assert!(output.errors.is_empty(), "{sql}");
        assert_eq!(
            output.table_lineage.in_tables,
            vec![table("p.d.t")],
            "{sql}"
        );
        assert_eq!(
            output.column_lineage,
            vec![ColumnLineage {
                descendant: ColumnMeta::new("value".to_string(), None),
                lineage: vec![],
            }],
            "{sql}"
        );
    }
}

#[test]
fn test_bigquery_unnest_alias_does_not_replace_qualified_column() {
    let output = test_sql_dialect(
        "SELECT currency AS generated, t.currency AS stored
         FROM p.d.t AS t CROSS JOIN UNNEST(['USD', 'GBP']) AS currency",
        "bigquery",
    )
    .unwrap();
    assert!(output.errors.is_empty());
    assert_eq!(
        output.column_lineage,
        vec![
            ColumnLineage {
                descendant: ColumnMeta::new("generated".to_string(), None),
                lineage: vec![],
            },
            ColumnLineage {
                descendant: ColumnMeta::new("stored".to_string(), None),
                lineage: vec![ColumnMeta::new(
                    "currency".to_string(),
                    Some(table("p.d.t"))
                )],
            },
        ]
    );
}

#[test]
fn test_bigquery_unnest_alias_scope() {
    for sql in [
        "SELECT s.currency AS value FROM UNNEST(['USD']) AS currency
         CROSS JOIN (SELECT currency FROM p.d.t) AS s",
        "SELECT currency AS value FROM UNNEST(['USD']) AS currency
         UNION ALL SELECT currency AS value FROM p.d.t",
        "WITH c AS (SELECT currency FROM UNNEST(['USD']) AS currency)
         SELECT currency AS value FROM p.d.t",
    ] {
        let output = test_sql_dialect(sql, "bigquery").unwrap();
        assert!(output.errors.is_empty(), "{sql}");
        assert_eq!(
            output.table_lineage.in_tables,
            vec![table("p.d.t")],
            "{sql}"
        );
        assert_eq!(
            output.column_lineage,
            vec![ColumnLineage {
                descendant: ColumnMeta::new("value".to_string(), None),
                lineage: vec![ColumnMeta::new(
                    "currency".to_string(),
                    Some(table("p.d.t"))
                )],
            }],
            "{sql}"
        );
    }
}
