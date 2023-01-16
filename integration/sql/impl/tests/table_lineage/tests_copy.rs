// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use openlineage_sql::{parse_sql, SqlMeta, TableLineage};
use sqlparser::dialect::SnowflakeDialect;

#[test]
fn parse_copy_from() {
    let meta = parse_sql(
        "
            COPY INTO SCHEMA.SOME_MONITORING_SYSTEM
                FROM (
                SELECT
                t.$1:st AS st,
                t.$1:index AS index,
                t.$1:cid AS cid,
                t.$1:k8s AS k8s,
                t.$1:cn AS cn,
                t.$1:did AS did,
                t.$1:tid AS tid,
                t.$1:tn AS tn,
                t.$1:mt AS mt,
                t.$1:op AS op,
                t.$1:drid AS drid,
                t.$1:mi AS mi,
                t.$1:q3dm17 AS q3dm17,
                t.$1:rsd AS rsd,
                t.$1:red AS red,
                t.$1:rd AS rd,
                t.$1:state AS state,
                t.$1:es AS es,
                t.$1:pool AS pool,
                t.$1:queue AS queue,
                t.$1:pw AS pw,
                metadata$fn AS load_fn,
                metadata$frn AS load_filerow,
                CURRENT_TIMESTAMP AS lts
                FROM @schema.general_finished AS t
            )",
        &SnowflakeDialect {},
        None,
    )
    .unwrap();
    assert_eq!(meta.errors.len(), 1);
    assert_eq!(
        meta.errors.get(0).unwrap().message,
        "Expected FROM or TO, found: SCHEMA".to_string()
    )
}
