{{ config(materialized='table') }}

with customer_orders as (
    select
        customer_id,
        count(*) as order_count,
        sum(amount) as total_amount,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from {{ ref('stg_orders') }}
    group by customer_id
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.created_at,
    coalesce(co.order_count, 0) as order_count,
    coalesce(co.total_amount, 0) as total_amount,
    co.first_order_date,
    co.last_order_date
from {{ ref('stg_customers') }} c
left join customer_orders co on c.customer_id = co.customer_id