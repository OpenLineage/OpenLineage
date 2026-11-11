{{ config(materialized='ephemeral') }}
select customer_id, sum(amount) as total from {{ ref('stg_orders') }} group by customer_id
