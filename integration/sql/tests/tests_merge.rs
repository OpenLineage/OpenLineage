use openlineage_sql::{parse_sql, SqlMeta};

#[test]
fn merge_subquery_when_not_matched() {
    assert_eq!(
        parse_sql(
            "
        MERGE INTO s.bar as dest
        USING (
            SELECT *
            FROM
            s.foo
        ) as stg
        ON dest.D = stg.D
          AND dest.E = stg.E
        WHEN NOT MATCHED THEN
        INSERT (
          A,
          B,
          C)
        VALUES
        (
            stg.A
            ,stg.B
            ,stg.C
        )"
        )
        .unwrap(),
        SqlMeta {
            in_tables: vec![String::from("s.foo")],
            out_tables: Some(String::from("s.bar"))
        }
    );
}
