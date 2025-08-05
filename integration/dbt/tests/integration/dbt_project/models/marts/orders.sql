{{ config(materialized='table') }}

with payment_totals as (
    select
        order_id,
        sum(amount) as total_payment_amount,
        count(*) as payment_count
    from {{ ref('stg_payments') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    o.amount as order_amount,
    coalesce(pt.total_payment_amount, 0) as payment_amount,
    coalesce(pt.payment_count, 0) as payment_count,
    case 
        when coalesce(pt.total_payment_amount, 0) >= o.amount then 'paid'
        when coalesce(pt.total_payment_amount, 0) > 0 then 'partial'
        else 'unpaid'
    end as payment_status
from {{ ref('stg_orders') }} o
left join payment_totals pt on o.order_id = pt.order_id