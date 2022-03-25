use openlineage_sql::{parse_sql, DbTableMeta, SqlMeta};
use sqlparser::dialect::PostgreSqlDialect;

pub fn test_sql(sql: &str) -> SqlMeta {
    parse_sql(sql, Box::new(PostgreSqlDialect {}), None).unwrap()
}

pub fn table(name: &str) -> Vec<DbTableMeta> {
    vec![DbTableMeta::py_new(String::from(name))]
}

pub fn tables(names: Vec<&str>) -> Vec<DbTableMeta> {
    names
        .into_iter()
        .map(|x| DbTableMeta::py_new(String::from(x)))
        .collect()
}
