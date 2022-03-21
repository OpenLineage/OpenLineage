use openlineage_sql::{parse_sql, QueryMetadata};

#[test]
fn test_tpcds_cte_query() {
    assert_eq!(parse_sql("
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
        ").unwrap(), QueryMetadata {
        inputs: vec![
            String::from("date_dim"),
            String::from("src.customer"),
            String::from("store_sales"),
        ],
        output: None,
    })
}

#[test]
fn test_tcpds_query_1() {
    assert_eq!(
        parse_sql(
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
        LIMIT 100;"
        )
        .unwrap(),
        QueryMetadata {
            inputs: vec![
                String::from("customer"),
                String::from("date_dim"),
                String::from("store"),
                String::from("store_returns"),
            ],
            output: None
        }
    )
}

#[test]
fn test_tcpds_query_2() {
    assert_eq!(
        parse_sql(
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
    ORDER  BY d_week_seq1;"
        )
        .unwrap(),
        QueryMetadata {
            inputs: vec![
                String::from("catalog_sales"),
                String::from("date_dim"),
                String::from("web_sales"),
            ],
            output: None
        }
    )
}

#[test]
fn test_tcpds_query_3() {
    assert_eq!(
        parse_sql(
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
        "
        )
        .unwrap(),
        QueryMetadata {
            inputs: vec![
                String::from("date_dim"),
                String::from("item"),
                String::from("store_sales"),
            ],
            output: None
        }
    )
}

#[test]
fn test_tcpds_query_4() {
    assert_eq!(
        parse_sql(
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
        "
        )
        .unwrap(),
        QueryMetadata {
            inputs: vec![
                String::from("catalog_sales"),
                String::from("customer"),
                String::from("date_dim"),
                String::from("store_sales"),
                String::from("web_sales")
            ],
            output: None
        }
    )
}
