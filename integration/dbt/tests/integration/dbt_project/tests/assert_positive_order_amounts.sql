-- Singular test: all orders should have positive amounts
select *
from {{ ref('orders') }}
where order_amount <= 0
