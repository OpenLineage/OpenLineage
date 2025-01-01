// Copyright 2018-2025 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

use crate::test_utils::*;
use openlineage_sql::TableLineage;

#[test]
fn test_tpcds_cte_query() {
    assert_eq!(test_sql("
            WITH year_total AS
                (SELECT c_customer_id customer_id,
                        c_first_name customer_first_name,
                        c_last_name customer_last_name,
                        c_preferred_cust_flag customer_preferred_cust_flag,
                        c_birth_country customer_birth_country,
                        c_login customer_login,
                        c_email_address customer_email_address,
                        d_year dyear,
                        Sum(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt)
                            + ss_ext_sales_price) / 2) year_total,
                        's' sale_type
                FROM src.customer,
                    store_sales,
                    date_dim
                WHERE c_customer_sk = ss_customer_sk
                    AND ss_sold_date_sk = d_date_sk GROUP  BY c_customer_id,
                                                            c_first_name,
                                                            c_last_name,
                                                            c_preferred_cust_flag,
                                                            c_birth_country,
                                                            c_login,
                                                            c_email_address,
                                                            d_year)
            SELECT t_s_secyear.customer_id,
                t_s_secyear.customer_first_name,
                t_s_secyear.customer_last_name,
                t_s_secyear.customer_preferred_cust_flag
            FROM year_total t_s_firstyear,
                year_total t_s_secyear,
                year_total t_c_firstyear,
                year_total t_c_secyear,
                year_total t_w_firstyear,
                year_total t_w_secyear
            WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
                AND t_s_firstyear.customer_id = t_c_secyear.customer_id
                AND t_s_firstyear.customer_id = t_c_firstyear.customer_id
                AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
                AND t_s_firstyear.customer_id = t_w_secyear.customer_id
                AND t_s_firstyear.sale_type = 's'
                AND t_c_firstyear.sale_type = 'c'
                AND t_w_firstyear.sale_type = 'w'
                AND t_s_secyear.sale_type = 's'
                AND t_c_secyear.sale_type = 'c'
                AND t_w_secyear.sale_type = 'w'
                AND t_s_firstyear.dyear = 2001
                AND t_s_secyear.dyear = 2001 + 1
                AND t_c_firstyear.dyear = 2001
                AND t_c_secyear.dyear = 2001 + 1
                AND t_w_firstyear.dyear = 2001
                AND t_w_secyear.dyear = 2001 + 1
                AND t_s_firstyear.year_total > 0
                AND t_c_firstyear.year_total > 0
                AND t_w_firstyear.year_total > 0
                AND CASE WHEN
                        t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
                        ELSE NULL
                    END > CASE WHEN
                        t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
                        ELSE NULL
                    END
                AND CASE WHEN
                        t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
                        ELSE NULL
                    END > CASE WHEN
                        t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total
                        ELSE NULL
                    END
                ORDER  BY t_s_secyear.customer_id,
                        t_s_secyear.customer_first_name,
                        t_s_secyear.customer_last_name,
                        t_s_secyear.customer_preferred_cust_flag
            LIMIT 100;
            ",
    ).unwrap().table_lineage, TableLineage {
        in_tables: tables(vec![
            "date_dim",
            "store_sales",
            "src.customer",
        ]),
        out_tables: vec![],
    })
}

#[test]
fn test_tcpds_query_1() {
    assert_eq!(
        test_sql(
            "
        WITH customer_total_return
             AS (SELECT sr_customer_sk     AS ctr_customer_sk,
                        sr_store_sk        AS ctr_store_sk,
                        Sum(sr_return_amt) AS ctr_total_return
                 FROM   store_returns,
                        date_dim
                 WHERE  sr_returned_date_sk = d_date_sk
                        AND d_year = 2001
                 GROUP  BY sr_customer_sk,
                           sr_store_sk)
        SELECT c_customer_id
        FROM   customer_total_return ctr1,
               store,
               customer
        WHERE  ctr1.ctr_total_return > (SELECT Avg(ctr_total_return) * 1.2
                                        FROM   customer_total_return ctr2
                                        WHERE  ctr1.ctr_store_sk = ctr2.ctr_store_sk)
               AND s_store_sk = ctr1.ctr_store_sk
               AND s_state = 'TN'
               AND ctr1.ctr_customer_sk = c_customer_sk
        ORDER  BY c_customer_id
        LIMIT 100;",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["customer", "date_dim", "store", "store_returns",]),
            out_tables: vec![]
        }
    )
}

#[test]
fn test_tcpds_query_2() {
    assert_eq!(
        test_sql(
            "
    WITH wscs
         AS (SELECT sold_date_sk,
                    sales_price
             FROM   (SELECT ws_sold_date_sk    sold_date_sk,
                            ws_ext_sales_price sales_price
                     FROM   web_sales)
             UNION ALL
             (SELECT cs_sold_date_sk    sold_date_sk,
                     cs_ext_sales_price sales_price
              FROM   catalog_sales)),
         wswscs
         AS (SELECT d_week_seq,
                    Sum(CASE
                          WHEN ( d_day_name = 'Sunday' ) THEN sales_price
                          ELSE NULL
                        END) sun_sales,
                    Sum(CASE
                          WHEN ( d_day_name = 'Monday' ) THEN sales_price
                          ELSE NULL
                        END) mon_sales,
                    Sum(CASE
                          WHEN ( d_day_name = 'Tuesday' ) THEN sales_price
                          ELSE NULL
                        END) tue_sales,
                    Sum(CASE
                          WHEN ( d_day_name = 'Wednesday' ) THEN sales_price
                          ELSE NULL
                        END) wed_sales,
                    Sum(CASE
                          WHEN ( d_day_name = 'Thursday' ) THEN sales_price
                          ELSE NULL
                        END) thu_sales,
                    Sum(CASE
                          WHEN ( d_day_name = 'Friday' ) THEN sales_price
                          ELSE NULL
                        END) fri_sales,
                    Sum(CASE
                          WHEN ( d_day_name = 'Saturday' ) THEN sales_price
                          ELSE NULL
                        END) sat_sales
             FROM   wscs,
                    date_dim
             WHERE  d_date_sk = sold_date_sk
             GROUP  BY d_week_seq)
    SELECT d_week_seq1,
           Round(sun_sales1 / sun_sales2, 2),
           Round(mon_sales1 / mon_sales2, 2),
           Round(tue_sales1 / tue_sales2, 2),
           Round(wed_sales1 / wed_sales2, 2),
           Round(thu_sales1 / thu_sales2, 2),
           Round(fri_sales1 / fri_sales2, 2),
           Round(sat_sales1 / sat_sales2, 2)
    FROM   (SELECT wswscs.d_week_seq d_week_seq1,
                   sun_sales         sun_sales1,
                   mon_sales         mon_sales1,
                   tue_sales         tue_sales1,
                   wed_sales         wed_sales1,
                   thu_sales         thu_sales1,
                   fri_sales         fri_sales1,
                   sat_sales         sat_sales1
            FROM   wswscs,
                   date_dim
            WHERE  date_dim.d_week_seq = wswscs.d_week_seq
                   AND d_year = 1998) y,
           (SELECT wswscs.d_week_seq d_week_seq2,
                   sun_sales         sun_sales2,
                   mon_sales         mon_sales2,
                   tue_sales         tue_sales2,
                   wed_sales         wed_sales2,
                   thu_sales         thu_sales2,
                   fri_sales         fri_sales2,
                   sat_sales         sat_sales2
            FROM   wswscs,
                   date_dim
            WHERE  date_dim.d_week_seq = wswscs.d_week_seq
                   AND d_year = 1998 + 1) z
    WHERE  d_week_seq1 = d_week_seq2 - 53
    ORDER  BY d_week_seq1;",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["catalog_sales", "date_dim", "web_sales",]),
            out_tables: vec![]
        }
    )
}

#[test]
fn test_tcpds_query_3() {
    assert_eq!(
        test_sql(
            "
        SELECT dt.d_year,
               item.i_brand_id          brand_id,
               item.i_brand             brand,
               Sum(ss_ext_discount_amt) sum_agg
        FROM   date_dim dt,
               store_sales,
               item
        WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk
               AND store_sales.ss_item_sk = item.i_item_sk
               AND item.i_manufact_id = 427
               AND dt.d_moy = 11
        GROUP  BY dt.d_year,
                  item.i_brand,
                  item.i_brand_id
        ORDER  BY dt.d_year,
                  sum_agg DESC,
                  brand_id
        LIMIT 100;
        ",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["date_dim", "item", "store_sales",]),
            out_tables: vec![]
        }
    )
}

#[test]
fn test_tcpds_query_4() {
    assert_eq!(
        test_sql(
            "
            WITH year_total
             AS (SELECT c_customer_id                       customer_id,
                        c_first_name                        customer_first_name,
                        c_last_name                         customer_last_name,
                        c_preferred_cust_flag               customer_preferred_cust_flag
                        ,
                        c_birth_country
                        customer_birth_country,
                        c_login                             customer_login,
                        c_email_address                     customer_email_address,
                        d_year                              dyear,
                        Sum(( ( ss_ext_list_price - ss_ext_wholesale_cost
                                - ss_ext_discount_amt
                              )
                              +
                                  ss_ext_sales_price ) / 2) year_total,
                        's'                                 sale_type
                 FROM   customer,
                        store_sales,
                        date_dim
                 WHERE  c_customer_sk = ss_customer_sk
                        AND ss_sold_date_sk = d_date_sk
                 GROUP  BY c_customer_id,
                           c_first_name,
                           c_last_name,
                           c_preferred_cust_flag,
                           c_birth_country,
                           c_login,
                           c_email_address,
                           d_year
                 UNION ALL
                 SELECT c_customer_id                             customer_id,
                        c_first_name                              customer_first_name,
                        c_last_name                               customer_last_name,
                        c_preferred_cust_flag
                        customer_preferred_cust_flag,
                        c_birth_country                           customer_birth_country
                        ,
                        c_login
                        customer_login,
                        c_email_address                           customer_email_address
                        ,
                        d_year                                    dyear
                        ,
                        Sum(( ( ( cs_ext_list_price
                                  - cs_ext_wholesale_cost
                                  - cs_ext_discount_amt
                                ) +
                                      cs_ext_sales_price ) / 2 )) year_total,
                        'c'                                       sale_type
                 FROM   customer,
                        catalog_sales,
                        date_dim
                 WHERE  c_customer_sk = cs_bill_customer_sk
                        AND cs_sold_date_sk = d_date_sk
                 GROUP  BY c_customer_id,
                           c_first_name,
                           c_last_name,
                           c_preferred_cust_flag,
                           c_birth_country,
                           c_login,
                           c_email_address,
                           d_year
                 UNION ALL
                 SELECT c_customer_id                             customer_id,
                        c_first_name                              customer_first_name,
                        c_last_name                               customer_last_name,
                        c_preferred_cust_flag
                        customer_preferred_cust_flag,
                        c_birth_country                           customer_birth_country
                        ,
                        c_login
                        customer_login,
                        c_email_address                           customer_email_address
                        ,
                        d_year                                    dyear
                        ,
                        Sum(( ( ( ws_ext_list_price
                                  - ws_ext_wholesale_cost
                                  - ws_ext_discount_amt
                                ) +
                                      ws_ext_sales_price ) / 2 )) year_total,
                        'w'                                       sale_type
                 FROM   customer,
                        web_sales,
                        date_dim
                 WHERE  c_customer_sk = ws_bill_customer_sk
                        AND ws_sold_date_sk = d_date_sk
                 GROUP  BY c_customer_id,
                           c_first_name,
                           c_last_name,
                           c_preferred_cust_flag,
                           c_birth_country,
                           c_login,
                           c_email_address,
                           d_year)
        SELECT t_s_secyear.customer_id,
                       t_s_secyear.customer_first_name,
                       t_s_secyear.customer_last_name,
                       t_s_secyear.customer_preferred_cust_flag
        FROM   year_total t_s_firstyear,
               year_total t_s_secyear,
               year_total t_c_firstyear,
               year_total t_c_secyear,
               year_total t_w_firstyear,
               year_total t_w_secyear
        WHERE  t_s_secyear.customer_id = t_s_firstyear.customer_id
               AND t_s_firstyear.customer_id = t_c_secyear.customer_id
               AND t_s_firstyear.customer_id = t_c_firstyear.customer_id
               AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
               AND t_s_firstyear.customer_id = t_w_secyear.customer_id
               AND t_s_firstyear.sale_type = 's'
               AND t_c_firstyear.sale_type = 'c'
               AND t_w_firstyear.sale_type = 'w'
               AND t_s_secyear.sale_type = 's'
               AND t_c_secyear.sale_type = 'c'
               AND t_w_secyear.sale_type = 'w'
               AND t_s_firstyear.dyear = 2001
               AND t_s_secyear.dyear = 2001 + 1
               AND t_c_firstyear.dyear = 2001
               AND t_c_secyear.dyear = 2001 + 1
               AND t_w_firstyear.dyear = 2001
               AND t_w_secyear.dyear = 2001 + 1
               AND t_s_firstyear.year_total > 0
               AND t_c_firstyear.year_total > 0
               AND t_w_firstyear.year_total > 0
               AND CASE
                     WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total /
                                                            t_c_firstyear.year_total
                     ELSE NULL
                   END > CASE
                           WHEN t_s_firstyear.year_total > 0 THEN
                           t_s_secyear.year_total /
                           t_s_firstyear.year_total
                           ELSE NULL
                         END
               AND CASE
                     WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total /
                                                            t_c_firstyear.year_total
                     ELSE NULL
                   END > CASE
                           WHEN t_w_firstyear.year_total > 0 THEN
                           t_w_secyear.year_total /
                           t_w_firstyear.year_total
                           ELSE NULL
                         END
        ORDER  BY t_s_secyear.customer_id,
                  t_s_secyear.customer_first_name,
                  t_s_secyear.customer_last_name,
                  t_s_secyear.customer_preferred_cust_flag
        LIMIT 100;
        ",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec![
                "catalog_sales",
                "customer",
                "date_dim",
                "store_sales",
                "web_sales"
            ]),
            out_tables: vec![]
        }
    )
}

#[test]
fn test_tcpds_query_5() {
    assert_eq!(test_sql("
            WITH ssr AS
            (
                     SELECT   s_store_id,
                              Sum(sales_price) AS sales,
                              Sum(profit)      AS profit,
                              Sum(return_amt)  AS returns1,
                              Sum(net_loss)    AS profit_loss
                     FROM     (
                                     SELECT ss_store_sk             AS store_sk,
                                            ss_sold_date_sk         AS date_sk,
                                            ss_ext_sales_price      AS sales_price,
                                            ss_net_profit           AS profit,
                                            Cast(0 AS DECIMAL(7,2)) AS return_amt,
                                            Cast(0 AS DECIMAL(7,2)) AS net_loss
                                     FROM   store_sales
                                     UNION ALL
                                     SELECT sr_store_sk             AS store_sk,
                                            sr_returned_date_sk     AS date_sk,
                                            Cast(0 AS DECIMAL(7,2)) AS sales_price,
                                            Cast(0 AS DECIMAL(7,2)) AS profit,
                                            sr_return_amt           AS return_amt,
                                            sr_net_loss             AS net_loss
                                     FROM   store_returns ) salesreturns,
                              date_dim,
                              store
                     WHERE    date_sk = d_date_sk
                     AND      d_date BETWEEN Cast('2002-08-22' AS DATE) AND      (
                                       Cast('2002-08-22' AS DATE) + INTERVAL '14' day)
                     AND      store_sk = s_store_sk
                     GROUP BY s_store_id) , csr AS
            (
                     SELECT   cp_catalog_page_id,
                              sum(sales_price) AS sales,
                              sum(profit)      AS profit,
                              sum(return_amt)  AS returns1,
                              sum(net_loss)    AS profit_loss
                     FROM     (
                                     SELECT cs_catalog_page_sk      AS page_sk,
                                            cs_sold_date_sk         AS date_sk,
                                            cs_ext_sales_price      AS sales_price,
                                            cs_net_profit           AS profit,
                                            cast(0 AS decimal(7,2)) AS return_amt,
                                            cast(0 AS decimal(7,2)) AS net_loss
                                     FROM   catalog_sales
                                     UNION ALL
                                     SELECT cr_catalog_page_sk      AS page_sk,
                                            cr_returned_date_sk     AS date_sk,
                                            cast(0 AS decimal(7,2)) AS sales_price,
                                            cast(0 AS decimal(7,2)) AS profit,
                                            cr_return_amount        AS return_amt,
                                            cr_net_loss             AS net_loss
                                     FROM   catalog_returns ) salesreturns,
                              date_dim,
                              catalog_page
                     WHERE    date_sk = d_date_sk
                     AND      d_date BETWEEN cast('2002-08-22' AS date) AND      (
                                       cast('2002-08-22' AS date) + INTERVAL '14' day)
                     AND      page_sk = cp_catalog_page_sk
                     GROUP BY cp_catalog_page_id) , wsr AS
            (
                     SELECT   web_site_id,
                              sum(sales_price) AS sales,
                              sum(profit)      AS profit,
                              sum(return_amt)  AS returns1,
                              sum(net_loss)    AS profit_loss
                     FROM     (
                                     SELECT ws_web_site_sk          AS wsr_web_site_sk,
                                            ws_sold_date_sk         AS date_sk,
                                            ws_ext_sales_price      AS sales_price,
                                            ws_net_profit           AS profit,
                                            cast(0 AS decimal(7,2)) AS return_amt,
                                            cast(0 AS decimal(7,2)) AS net_loss
                                     FROM   web_sales
                                     UNION ALL
                                     SELECT          ws_web_site_sk          AS wsr_web_site_sk,
                                                     wr_returned_date_sk     AS date_sk,
                                                     cast(0 AS decimal(7,2)) AS sales_price,
                                                     cast(0 AS decimal(7,2)) AS profit,
                                                     wr_return_amt           AS return_amt,
                                                     wr_net_loss             AS net_loss
                                     FROM            web_returns
                                     LEFT OUTER JOIN web_sales
                                     ON              (
                                                                     wr_item_sk = ws_item_sk
                                                     AND             wr_order_number = ws_order_number) ) salesreturns,
                              date_dim,
                              web_site
                     WHERE    date_sk = d_date_sk
                     AND      d_date BETWEEN cast('2002-08-22' AS date) AND      (
                                       cast('2002-08-22' AS date) + INTERVAL '14' day)
                     AND      wsr_web_site_sk = web_site_sk
                     GROUP BY web_site_id)
            SELECT
                     channel ,
                     id ,
                     sum(sales)   AS sales ,
                     sum(returns1) AS returns1 ,
                     sum(profit)  AS profit
            FROM     (
                            SELECT 'store channel' AS channel ,
                                   'store'
                                          || s_store_id AS id ,
                                   sales ,
                                   returns1 ,
                                   (profit - profit_loss) AS profit
                            FROM   ssr
                            UNION ALL
                            SELECT 'catalog channel' AS channel ,
                                   'catalog_page'
                                          || cp_catalog_page_id AS id ,
                                   sales ,
                                   returns1 ,
                                   (profit - profit_loss) AS profit
                            FROM   csr
                            UNION ALL
                            SELECT 'web channel' AS channel ,
                                   'web_site'
                                          || web_site_id AS id ,
                                   sales ,
                                   returns1 ,
                                   (profit - profit_loss) AS profit
                            FROM   wsr ) x
            GROUP BY rollup (channel, id)
            ORDER BY channel ,
                     id
            LIMIT 100; ",
    ).unwrap().table_lineage, TableLineage {
        in_tables: tables(vec![
            "catalog_page",
            "catalog_returns",
            "catalog_sales",
            "date_dim",
            "store",
            "store_returns",
            "store_sales",
            "web_returns",
            "web_sales",
            "web_site"
        ]),
        out_tables: vec![]
    })
}

#[test]
fn test_tcpds_query_6() {
    assert_eq!(
        test_sql(
            "
        SELECT a.ca_state state,
               Count(*)   cnt
        FROM   customer_address a,
               customer c,
               store_sales s,
               date_dim d,
               item i
        WHERE  a.ca_address_sk = c.c_current_addr_sk
               AND c.c_customer_sk = s.ss_customer_sk
               AND s.ss_sold_date_sk = d.d_date_sk
               AND s.ss_item_sk = i.i_item_sk
               AND d.d_month_seq = (SELECT DISTINCT ( d_month_seq )
                                    FROM   date_dim
                                    WHERE  d_year = 1998
                                           AND d_moy = 7)
               AND i.i_current_price > 1.2 * (SELECT Avg(j.i_current_price)
                                              FROM   item j
                                              WHERE  j.i_category = i.i_category)
        GROUP  BY a.ca_state
        HAVING Count(*) >= 10
        ORDER  BY cnt
        LIMIT 100;
    ",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec![
                "customer",
                "customer_address",
                "date_dim",
                "item",
                "store_sales",
            ]),
            out_tables: vec![]
        }
    )
}

#[test]
fn test_tcpds_query_7() {
    assert_eq!(
        test_sql(
            "
        SELECT i_item_id,
               Avg(ss_quantity)    agg1,
               Avg(ss_list_price)  agg2,
               Avg(ss_coupon_amt)  agg3,
               Avg(ss_sales_price) agg4
        FROM   store_sales,
               customer_demographics,
               date_dim,
               item,
               promotion
        WHERE  ss_sold_date_sk = d_date_sk
               AND ss_item_sk = i_item_sk
               AND ss_cdemo_sk = cd_demo_sk
               AND ss_promo_sk = p_promo_sk
               AND cd_gender = 'F'
               AND cd_marital_status = 'W'
               AND cd_education_status = '2 yr Degree'
               AND ( p_channel_email = 'N'
                      OR p_channel_event = 'N' )
               AND d_year = 1998
        GROUP  BY i_item_id
        ORDER  BY i_item_id
        LIMIT 100;
    ",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec![
                "customer_demographics",
                "date_dim",
                "item",
                "promotion",
                "store_sales",
            ]),
            out_tables: vec![]
        }
    )
}

#[test]
fn test_tcpds_query_8() {
    assert_eq!(
        test_sql(
            "
        SELECT s_store_name,
                       Sum(ss_net_profit)
        FROM   store_sales,
               date_dim,
               store,
               (SELECT ca_zip
                FROM   (SELECT Substr(ca_zip, 1, 5) ca_zip
                        FROM   customer_address
                        WHERE  Substr(ca_zip, 1, 5) IN ( '67436', '26121', '38443',
                                                         '63157',
                                                         '68856', '19485', '86425',
                                                         '26741',
                                                         '70991', '60899', '63573',
                                                         '47556',
                                                         '56193', '93314', '87827',
                                                         '62017',
                                                         '85067', '95390', '48091',
                                                         '43224',
                                                         '22322', '86959', '68519',
                                                         '14308',
                                                         '46501', '81131', '34056',
                                                         '61991',
                                                         '19896', '87804', '65774',
                                                         '92564' )
                        INTERSECT
                        SELECT ca_zip
                        FROM   (SELECT Substr(ca_zip, 1, 5) ca_zip,
                                       Count(*)             cnt
                                FROM   customer_address,
                                       customer
                                WHERE  ca_address_sk = c_current_addr_sk
                                       AND c_preferred_cust_flag = 'Y'
                                GROUP  BY ca_zip
                                HAVING Count(*) > 10)A1)A2) V1
        WHERE  ss_store_sk = s_store_sk
               AND ss_sold_date_sk = d_date_sk
               AND d_qoy = 2
               AND d_year = 2000
               AND ( Substr(s_zip, 1, 2) = Substr(V1.ca_zip, 1, 2) )
        GROUP  BY s_store_name
        ORDER  BY s_store_name
        LIMIT 100;
    ",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec![
                "customer",
                "customer_address",
                "date_dim",
                "store",
                "store_sales",
            ]),
            out_tables: vec![]
        }
    )
}

#[test]
fn test_tcpds_query_9() {
    assert_eq!(
        test_sql(
            "
        SELECT CASE
                 WHEN (SELECT Count(*)
                       FROM   store_sales
                       WHERE  ss_quantity BETWEEN 1 AND 20) > 3672 THEN
                 (SELECT Avg(ss_ext_list_price)
                  FROM   store_sales
                  WHERE
                 ss_quantity BETWEEN 1 AND 20)
                 ELSE (SELECT Avg(ss_net_profit)
                       FROM   store_sales
                       WHERE  ss_quantity BETWEEN 1 AND 20)
               END bucket1,
               CASE
                 WHEN (SELECT Count(*)
                       FROM   store_sales
                       WHERE  ss_quantity BETWEEN 21 AND 40) > 3392 THEN
                 (SELECT Avg(ss_ext_list_price)
                  FROM   store_sales
                  WHERE
                 ss_quantity BETWEEN 21 AND 40)
                 ELSE (SELECT Avg(ss_net_profit)
                       FROM   store_sales
                       WHERE  ss_quantity BETWEEN 21 AND 40)
               END bucket2,
               CASE
                 WHEN (SELECT Count(*)
                       FROM   store_sales
                       WHERE  ss_quantity BETWEEN 41 AND 60) > 32784 THEN
                 (SELECT Avg(ss_ext_list_price)
                  FROM   store_sales
                  WHERE
                 ss_quantity BETWEEN 41 AND 60)
                 ELSE (SELECT Avg(ss_net_profit)
                       FROM   store_sales
                       WHERE  ss_quantity BETWEEN 41 AND 60)
               END bucket3,
               CASE
                 WHEN (SELECT Count(*)
                       FROM   store_sales
                       WHERE  ss_quantity BETWEEN 61 AND 80) > 26032 THEN
                 (SELECT Avg(ss_ext_list_price)
                  FROM   store_sales
                  WHERE
                 ss_quantity BETWEEN 61 AND 80)
                 ELSE (SELECT Avg(ss_net_profit)
                       FROM   store_sales
                       WHERE  ss_quantity BETWEEN 61 AND 80)
               END bucket4,
               CASE
                 WHEN (SELECT Count(*)
                       FROM   store_sales
                       WHERE  ss_quantity BETWEEN 81 AND 100) > 23982 THEN
                 (SELECT Avg(ss_ext_list_price)
                  FROM   store_sales
                  WHERE
                 ss_quantity BETWEEN 81 AND 100)
                 ELSE (SELECT Avg(ss_net_profit)
                       FROM   store_sales
                       WHERE  ss_quantity BETWEEN 81 AND 100)
               END bucket5
        FROM   reason
        WHERE  r_reason_sk = 1;
    ",
        )
        .unwrap()
        .table_lineage,
        TableLineage {
            in_tables: tables(vec!["reason", "store_sales"]),
            out_tables: vec![]
        }
    )
}
