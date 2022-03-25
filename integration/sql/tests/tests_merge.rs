#[macro_use]
mod test_utils;

use openlineage_sql::SqlMeta;
use test_utils::*;

#[test]
fn merge_subquery_when_not_matched() {
    assert_eq!(
        test_sql(
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
        ),
        SqlMeta {
            in_tables: table("s.foo"),
            out_tables: table("s.bar")
        }
    );
}
