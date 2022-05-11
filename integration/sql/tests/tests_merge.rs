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
        )",
        ),
        SqlMeta {
            in_tables: table("s.foo"),
            out_tables: table("s.bar")
        }
    );
}

// QUALIFY expression is not yet supported
#[ignore]
#[test]
fn test_merge_multiple_clauses() {
    assert_eq!(
        test_sql(
            "
            merge into \"m\".\"d\" as d_m using (
                with f_d_u_l_u as (
                    select r_number() over (partition by m_id order by c_dt desc) as rownum,
                        *
                    from \"c\".\"u_l_u\"
                    where len(m_id) = 32
                    and len(c_name) > 1
                    and p_at = '2022-04-14'
                        qualify rownum = 1
                )
                select
                c,
                c_code,
                c_name,
                r_code,
                r_name,
                z,
                m_id
                from f_d_u_l_u
            ) as src on d_m.m_id = src.m_id
            when matched then update set
            d_m.c_name = src.c_name,
            d_m.c_code = src.c_code,
            d_m.r_name = src.r_name,
            d_m.r_code = src.r_code,
            d_m.c = src.c,
            d_m.z = src.z
            when not matched then insert (m_id,c_name,c_code,r_name,r_code,c,z)
            values (m_id,c_name,c_code,r_name,r_code,c,z);"
        ),
        SqlMeta {
            in_tables: table("c.u_l_u"),
            out_tables: table("m.d")
        }
    )
}
