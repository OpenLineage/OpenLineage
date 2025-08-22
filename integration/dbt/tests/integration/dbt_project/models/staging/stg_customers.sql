{{ config(materialized='view') }}

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at
from {{ ref('raw_customers') }}