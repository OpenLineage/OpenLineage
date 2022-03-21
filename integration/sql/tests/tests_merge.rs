use openlineage_sql::{parse_sql, QueryMetadata};

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
        QueryMetadata {
            inputs: vec![String::from("s.foo")],
            output: Some(String::from("s.bar"))
        }
    );
}
