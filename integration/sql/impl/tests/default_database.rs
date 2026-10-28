// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use openlineage_sql::{
    get_dialect, parse_multiple_statements, parse_multiple_statements_with_default_database,
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
