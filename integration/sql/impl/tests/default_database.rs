// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use openlineage_sql::{
    get_dialect, parse_multiple_statements, parse_multiple_statements_with_default_database,
    DbTableMeta, SqlMeta,
};

#[test]
fn default_database_qualifies_table_and_column_lineage() {
    let metadata = parse_multiple_statements_with_default_database(
        vec!["INSERT INTO target SELECT id FROM source".to_string()],
        get_dialect("postgres"),
        Some("public".to_string()),
        Some("mydb".to_string()),
    )
    .unwrap();

    assert!(metadata.errors.is_empty());
    let input = &metadata.table_lineage.in_tables[0];
    let output = &metadata.table_lineage.out_tables[0];
    assert_eq!(input.database.as_deref(), Some("mydb"));
    assert_eq!(input.schema.as_deref(), Some("public"));
    assert_eq!(output.database.as_deref(), Some("mydb"));
    assert_eq!(output.schema.as_deref(), Some("public"));
    assert_eq!(metadata.column_lineage.len(), 1);
    assert_eq!(
        metadata.column_lineage[0].lineage[0].origin.as_ref(),
        Some(input)
    );
    assert_eq!(metadata.column_lineage[0].descendant.name, "id");
    assert!(metadata.column_lineage[0].descendant.origin.is_none());
}

#[test]
fn explicit_database_and_schema_override_defaults() {
    let metadata = parse_multiple_statements_with_default_database(
        vec!["SELECT id FROM otherdb.custom.source".to_string()],
        get_dialect("postgres"),
        Some("public".to_string()),
        Some("mydb".to_string()),
    )
    .unwrap();

    let input = &metadata.table_lineage.in_tables[0];
    assert_eq!(input.database.as_deref(), Some("otherdb"));
    assert_eq!(input.schema.as_deref(), Some("custom"));
    assert_eq!(
        metadata.column_lineage[0].lineage[0].origin.as_ref(),
        Some(input)
    );
}

#[test]
fn use_database_overrides_initial_default() {
    let metadata = parse_multiple_statements_with_default_database(
        vec!["USE otherdb; SELECT id FROM source".to_string()],
        get_dialect("generic"),
        Some("public".to_string()),
        Some("mydb".to_string()),
    )
    .unwrap();

    assert_eq!(
        metadata.table_lineage.in_tables[0].database.as_deref(),
        Some("otherdb")
    );
    assert_eq!(
        metadata.column_lineage[0].lineage[0].origin.as_ref(),
        Some(&metadata.table_lineage.in_tables[0])
    );
}

#[test]
fn absent_default_database_preserves_existing_behavior() {
    let sql = vec!["SELECT id FROM source".to_string()];
    let dialect = get_dialect("postgres");
    assert_eq!(
        parse_multiple_statements(sql.clone(), dialect, Some("public".to_string())).unwrap(),
        parse_multiple_statements_with_default_database(
            sql,
            dialect,
            Some("public".to_string()),
            None
        )
        .unwrap()
    );
}

fn table_names(tables: &[DbTableMeta]) -> Vec<String> {
    let mut names: Vec<String> = tables.iter().map(|t| t.qualified_name()).collect();
    names.sort();
    names
}

fn parse_cte_insert(default_schema: Option<&str>, default_database: Option<&str>) -> SqlMeta {
    parse_multiple_statements_with_default_database(
        vec!["WITH c AS (SELECT id FROM source) INSERT INTO target SELECT id FROM c".to_string()],
        get_dialect("postgres"),
        default_schema.map(str::to_string),
        default_database.map(str::to_string),
    )
    .unwrap()
}

#[test]
fn cte_sources_are_inputs_without_defaults() {
    let metadata = parse_cte_insert(None, None);
    assert!(metadata.errors.is_empty());
    assert_eq!(table_names(&metadata.table_lineage.in_tables), ["source"]);
    assert_eq!(table_names(&metadata.table_lineage.out_tables), ["target"]);
}

#[test]
fn cte_sources_are_inputs_with_default_schema() {
    let metadata = parse_cte_insert(Some("public"), None);
    assert!(metadata.errors.is_empty());
    assert_eq!(
        table_names(&metadata.table_lineage.in_tables),
        ["public.source"]
    );
    assert_eq!(
        table_names(&metadata.table_lineage.out_tables),
        ["public.target"]
    );
}

#[test]
fn cte_sources_are_inputs_with_default_schema_and_database() {
    let metadata = parse_cte_insert(Some("public"), Some("mydb"));
    assert!(metadata.errors.is_empty());
    assert_eq!(
        table_names(&metadata.table_lineage.in_tables),
        ["mydb.public.source"]
    );
    assert_eq!(
        table_names(&metadata.table_lineage.out_tables),
        ["mydb.public.target"]
    );
}

#[test]
fn chained_cte_sources_are_inputs_with_defaults() {
    let metadata = parse_multiple_statements_with_default_database(
        vec![
            "WITH a AS (SELECT id FROM source), b AS (SELECT id FROM a) \
             INSERT INTO target SELECT id FROM b"
                .to_string(),
        ],
        get_dialect("postgres"),
        Some("public".to_string()),
        Some("mydb".to_string()),
    )
    .unwrap();
    assert!(metadata.errors.is_empty());
    assert_eq!(
        table_names(&metadata.table_lineage.in_tables),
        ["mydb.public.source"]
    );
}

fn parse_cte_inputs(sql: &str, default_schema: Option<&str>) -> Vec<String> {
    let metadata = parse_multiple_statements_with_default_database(
        vec![sql.to_string()],
        get_dialect("postgres"),
        default_schema.map(str::to_string),
        None,
    )
    .unwrap();
    assert!(metadata.errors.is_empty());
    table_names(&metadata.table_lineage.in_tables)
}

#[test]
fn qualified_source_with_cte_name_is_input() {
    let sql = "WITH orders AS (SELECT * FROM raw.orders) INSERT INTO target SELECT * FROM orders";
    assert_eq!(parse_cte_inputs(sql, None), ["raw.orders"]);
    assert_eq!(parse_cte_inputs(sql, Some("public")), ["raw.orders"]);
}

#[test]
fn default_qualified_source_with_cte_name_is_input() {
    let sql =
        "WITH orders AS (SELECT * FROM public.orders) INSERT INTO target SELECT * FROM orders";
    assert_eq!(parse_cte_inputs(sql, Some("public")), ["public.orders"]);
}

#[test]
fn non_recursive_cte_body_reads_table_with_its_own_name() {
    let sql = "WITH orders AS (SELECT * FROM orders) INSERT INTO target SELECT * FROM orders";
    assert_eq!(parse_cte_inputs(sql, None), ["orders"]);
    assert_eq!(parse_cte_inputs(sql, Some("public")), ["public.orders"]);
}

fn parse_inputs(sql: &str, dialect: &str, default_schema: Option<&str>) -> Vec<String> {
    let metadata = parse_multiple_statements_with_default_database(
        vec![sql.to_string()],
        get_dialect(dialect),
        default_schema.map(str::to_string),
        None,
    )
    .unwrap();
    assert!(metadata.errors.is_empty());
    table_names(&metadata.table_lineage.in_tables)
}

#[test]
fn recursive_cte_self_reference_is_not_an_input() {
    let sql = "WITH RECURSIVE t AS (SELECT n FROM source UNION ALL SELECT n FROM t) \
               INSERT INTO target SELECT n FROM t";
    assert_eq!(parse_inputs(sql, "postgres", None), ["source"]);
}

#[test]
fn implicit_recursive_cte_self_reference_is_not_an_input() {
    let sql = "WITH t AS (SELECT n FROM source UNION ALL SELECT n FROM t) \
               INSERT INTO target SELECT n FROM t";
    for dialect in ["snowflake", "mssql", "sqlite"] {
        assert_eq!(parse_inputs(sql, dialect, None), ["source"], "{dialect}");
    }
}

#[test]
fn qualified_source_with_earlier_cte_name_is_input() {
    let sql = "WITH a AS (SELECT id FROM source), b AS (SELECT id FROM raw.a) \
               INSERT INTO target SELECT id FROM b";
    assert_eq!(parse_cte_inputs(sql, None), ["raw.a"]);
    assert_eq!(parse_cte_inputs(sql, Some("public")), ["raw.a"]);
}

#[test]
fn default_qualified_source_with_recursive_cte_name_is_input() {
    // Defaults make `public.orders` and a bare `orders` the same table, so
    // only how the reference was written tells it apart from the CTE.
    let sql = "WITH RECURSIVE orders AS (SELECT * FROM public.orders) \
               INSERT INTO target SELECT * FROM orders";
    assert_eq!(
        parse_inputs(sql, "postgres", Some("public")),
        ["public.orders"]
    );
    let sql =
        "WITH orders AS (SELECT * FROM public.orders) INSERT INTO target SELECT * FROM orders";
    for dialect in ["snowflake", "mssql", "sqlite"] {
        assert_eq!(
            parse_inputs(sql, dialect, Some("public")),
            ["public.orders"],
            "{dialect}"
        );
    }
}

#[test]
fn default_qualified_source_with_earlier_cte_name_is_input() {
    let sql = "WITH a AS (SELECT id FROM source), b AS (SELECT id FROM public.a) \
               INSERT INTO target SELECT id FROM b";
    assert_eq!(parse_cte_inputs(sql, Some("public")), ["public.a"]);
}

#[test]
fn cte_reference_in_nested_subquery_is_resolved() {
    // A bare reference inside a subquery of a CTE body still names the CTE.
    let recursive = "WITH RECURSIVE t AS (SELECT n FROM source \
                     UNION ALL SELECT n FROM (SELECT n FROM t) AS sub) \
                     INSERT INTO target SELECT n FROM t";
    assert_eq!(
        parse_cte_inputs(recursive, Some("public")),
        ["public.source"]
    );
    let chained = "WITH a AS (SELECT id FROM source), \
                   b AS (SELECT id FROM (SELECT id FROM a) AS sub) \
                   INSERT INTO target SELECT id FROM b";
    assert_eq!(parse_cte_inputs(chained, Some("public")), ["public.source"]);
}

#[test]
fn qualified_reference_is_not_mistaken_for_cte() {
    let metadata = parse_multiple_statements_with_default_database(
        vec![
            "WITH c AS (SELECT id FROM source) INSERT INTO target SELECT id FROM other.c"
                .to_string(),
        ],
        get_dialect("postgres"),
        Some("public".to_string()),
        Some("mydb".to_string()),
    )
    .unwrap();
    assert!(metadata.errors.is_empty());
    assert_eq!(
        table_names(&metadata.table_lineage.in_tables),
        ["mydb.other.c"]
    );
}

/// For two-level dialects such as MySQL, a 2-part name `x.t` is parsed as
/// schema `x`. The connection's database must therefore be passed as
/// `default_schema` (not `default_database`) so that unqualified and
/// qualified references resolve to the same table.
#[test]
fn mysql_connection_database_is_passed_as_default_schema() {
    let metadata = parse_multiple_statements_with_default_database(
        vec!["INSERT INTO mydb.t SELECT * FROM t".to_string()],
        get_dialect("mysql"),
        Some("mydb".to_string()),
        None,
    )
    .unwrap();
    assert!(metadata.errors.is_empty());
    assert_eq!(table_names(&metadata.table_lineage.in_tables), ["mydb.t"]);
    assert_eq!(table_names(&metadata.table_lineage.out_tables), ["mydb.t"]);
}
