select id as order_id, customer_id, amount, status from {{ ref('raw_orders') }}
