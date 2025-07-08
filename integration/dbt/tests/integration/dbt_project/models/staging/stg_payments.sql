{{ config(materialized='view') }}

select
    payment_id,
    order_id,
    payment_method,
    amount,
    created_at
from {{ ref('raw_payments') }}