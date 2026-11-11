select id as customer_id, name, status from {{ ref('raw_customers') }}
