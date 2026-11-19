// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::{table, test_sql, test_sql_dialect};
use openlineage_sql::{ColumnLineage, ColumnMeta};

#[test]
fn test_simple_cte() {
    let output = test_sql(
        "WITH my_cte AS (
            SELECT a,b,c FROM t1
         )
         SELECT a,c FROM my_cte",
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
                    origin: Some(table("t1")),
                    name: "a".to_string()
                }]
            },
            ColumnLineage {
                descendant: ColumnMeta {
                    origin: None,
                    name: "c".to_string()
                },
                lineage: vec![ColumnMeta {
                    origin: Some(table("t1")),
                    name: "c".to_string()
                }]
            },
        ]
    );
}

fn column(name: &str, origin: &str) -> ColumnLineage {
    ColumnLineage {
        descendant: ColumnMeta {
            origin: None,
            name: name.to_string(),
        },
        lineage: vec![ColumnMeta {
            origin: Some(table(origin)),
            name: name.to_string(),
        }],
    }
}

#[test]
fn test_cte_select_wildcard_from_table() {
    let output = test_sql(
        "WITH source AS (SELECT * FROM raw_customers WHERE active)
         SELECT id, name FROM source",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            column("id", "raw_customers"),
            column("name", "raw_customers")
        ]
    );
}

#[test]
fn test_cte_select_wildcard_from_table_through_another_cte() {
    let output = test_sql(
        "WITH source AS (SELECT * FROM raw_customers),
              renamed AS (SELECT id AS customer_id FROM source)
         SELECT customer_id FROM renamed",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![ColumnLineage {
            descendant: ColumnMeta {
                origin: None,
                name: "customer_id".to_string()
            },
            lineage: vec![ColumnMeta {
                origin: Some(table("raw_customers")),
                name: "id".to_string()
            }]
        }]
    );
}

#[test]
fn test_cte_select_wildcard_from_cte() {
    let output = test_sql(
        "WITH base AS (SELECT a AS id FROM t1),
              source AS (SELECT * FROM base)
         SELECT id FROM source",
    )
    .unwrap();
    assert_eq!(
        output.column_lineage,
        vec![ColumnLineage {
            descendant: ColumnMeta {
                origin: None,
                name: "id".to_string()
            },
            lineage: vec![ColumnMeta {
                origin: Some(table("t1")),
                name: "a".to_string()
            }]
        }]
    );
}

#[test]
fn test_derived_table_select_wildcard_from_table() {
    let output = test_sql("SELECT s.id, s.name FROM (SELECT * FROM raw_customers) s").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            column("id", "raw_customers"),
            column("name", "raw_customers")
        ]
    );
}

#[test]
fn test_cte_select_wildcard_from_join_keeps_cte_origin() {
    let output = test_sql(
        "WITH source AS (
            SELECT * FROM raw_customers c JOIN raw_orders o ON c.id = o.customer_id
         )
         SELECT id FROM source",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![column("id", "source")]);
}

#[test]
fn test_cte_select_wildcard_with_modifier_keeps_cte_origin() {
    let output = test_sql_dialect(
        "WITH source AS (SELECT * EXCLUDE (secret) FROM raw_customers)
         SELECT id FROM source",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![column("id", "source")]);
}

#[test]
fn test_cte_select_wildcard_from_identifier_function() {
    let output = test_sql_dialect(
        "WITH source AS (SELECT * FROM IDENTIFIER('raw_customers'))
         SELECT id FROM source",
        "snowflake",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![column("id", "raw_customers")]);
}

#[test]
fn test_cte_select_wildcard_with_lateral_view_keeps_cte_origin() {
    let output = test_sql_dialect(
        "WITH source AS (SELECT * FROM t LATERAL VIEW explode(arr) x AS col)
         SELECT col FROM source",
        "hive",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![column("col", "source")]);
}

#[test]
fn test_cte_with_column_list_select_wildcard_keeps_cte_origin() {
    let output = test_sql("WITH source(x) AS (SELECT * FROM t) SELECT x FROM source").unwrap();
    assert_eq!(output.column_lineage, vec![column("x", "source")]);
}

#[test]
fn test_derived_table_with_column_list_select_wildcard_keeps_alias_origin() {
    let output = test_sql("SELECT s.x FROM (SELECT * FROM t) s(x)").unwrap();
    assert_eq!(output.column_lineage, vec![column("x", "s")]);
}

#[test]
fn test_cte_select_wildcard_from_table_with_column_list_keeps_cte_origin() {
    let output = test_sql("WITH source AS (SELECT * FROM t AS r(x)) SELECT x FROM source").unwrap();
    assert_eq!(output.column_lineage, vec![column("x", "source")]);
}

#[test]
fn test_cte_select_wildcard_not_shadowed_by_derived_table_alias_in_another_cte() {
    let output = test_sql(
        "WITH c AS (SELECT * FROM t1),
              d AS (SELECT * FROM (SELECT * FROM t2) c)
         SELECT id FROM c",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![column("id", "t1")]);
}

#[test]
fn test_cte_select_wildcard_from_table_named_like_later_cte() {
    let output = test_sql(
        "WITH a AS (SELECT * FROM b),
              b AS (SELECT * FROM a)
         SELECT id FROM b",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![column("id", "b")]);
}

#[test]
fn test_cte_reading_table_named_like_later_select_wildcard_cte() {
    let output = test_sql(
        "WITH a AS (SELECT id FROM b),
              b AS (SELECT * FROM c)
         SELECT id FROM a",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![column("id", "b")]);
}

#[test]
fn test_derived_table_select_wildcard_does_not_resolve_through_sibling() {
    for sql in [
        "SELECT t1.id FROM (SELECT * FROM t2) t1 JOIN (SELECT * FROM t1) t2 ON t1.id = t2.id",
        "SELECT t1.id FROM (SELECT * FROM t2) t1 JOIN (SELECT * FROM t3) t2 ON t1.id = t2.id",
        "SELECT t1.id FROM (SELECT id FROM t2) t1 JOIN (SELECT * FROM t3) t2 ON t1.id = t2.id",
    ] {
        let output = test_sql(sql).unwrap();
        assert_eq!(output.column_lineage, vec![column("id", "t2")], "{sql}");
    }
}

#[test]
fn test_derived_table_select_wildcard_unqualified_columns() {
    let output = test_sql("SELECT id, name FROM (SELECT * FROM raw_customers) s").unwrap();
    assert_eq!(
        output.column_lineage,
        vec![
            column("id", "raw_customers"),
            column("name", "raw_customers")
        ]
    );
}

#[test]
fn test_cte_select_wildcard_from_identifier_function_with_args() {
    for dialect in ["databricks", "mssql"] {
        let output = test_sql_dialect(
            "WITH source AS (SELECT * FROM IDENTIFIER('raw_customers'))
             SELECT id FROM source",
            dialect,
        )
        .unwrap();
        assert_eq!(
            output.column_lineage,
            vec![column("id", "raw_customers")],
            "{dialect}"
        );
    }
}

#[test]
fn test_cte_select_wildcard_from_table_function_keeps_cte_origin() {
    let output = test_sql_dialect(
        "WITH source AS (SELECT * FROM my_tvf(1)) SELECT id FROM source",
        "databricks",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![column("id", "source")]);
}

#[test]
fn test_cte_select_wildcard_not_shadowed_by_derived_table_alias_in_update_cte() {
    let output = test_sql(
        "WITH c AS (SELECT * FROM t1),
              d AS (UPDATE target SET x = c.x FROM (SELECT * FROM t2) c)
         SELECT id FROM c",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![column("id", "t1")]);
}

#[test]
fn test_cte_select_wildcard_alias_quoted_differently() {
    for sql in [
        r#"WITH "source" AS (SELECT * FROM raw_customers) SELECT id FROM source"#,
        r#"WITH source AS (SELECT * FROM raw_customers) SELECT id FROM "source""#,
    ] {
        let output = test_sql(sql).unwrap();
        assert_eq!(
            output.column_lineage,
            vec![column("id", "raw_customers")],
            "{sql}"
        );
    }
}

#[test]
fn test_cte_select_wildcard_changing_columns_keeps_cte_origin() {
    for (sql, dialect, name) in [
        (
            "WITH s AS (SELECT AS STRUCT * FROM raw_customers) SELECT id FROM s",
            "bigquery",
            "id",
        ),
        (
            "WITH s AS (SELECT * EXCLUDE secret FROM raw_customers) SELECT id FROM s",
            "redshift",
            "id",
        ),
        (
            "WITH s AS (SELECT * FROM raw_customers WITH ORDINALITY) SELECT ordinality FROM s",
            "postgres",
            "ordinality",
        ),
        (
            "WITH s AS (SELECT * FROM raw_customers[0]) SELECT id FROM s",
            "redshift",
            "id",
        ),
    ] {
        let output = test_sql_dialect(sql, dialect).unwrap();
        assert_eq!(output.column_lineage, vec![column(name, "s")], "{sql}");
    }
}

#[test]
fn test_cte_select_wildcard_from_table_named_like_later_explicit_cte() {
    let output = test_sql(
        "WITH a AS (SELECT * FROM b),
              b AS (SELECT id FROM c)
         SELECT id FROM a",
    )
    .unwrap();
    assert_eq!(output.column_lineage, vec![column("id", "b")]);
}

#[test]
fn test_cte_select_wildcard_alias_with_dot_is_not_a_qualified_table() {
    let output =
        test_sql(r#"WITH "s.t" AS (SELECT * FROM raw_customers) SELECT id FROM s.t"#).unwrap();
    assert_eq!(output.column_lineage, vec![column("id", "s.t")]);
}
