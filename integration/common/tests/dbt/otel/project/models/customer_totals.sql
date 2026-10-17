{{ config(materialized='table') }}
select c.customer_id, c.name, coalesce(t.total, 0) as total
from {{ ref('stg_customers') }} as c
left join {{ ref('eph_order_totals') }} as t on c.customer_id = t.customer_id
