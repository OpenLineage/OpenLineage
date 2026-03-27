-- Singular test joining two tables: customer order counts should match
select
    c.customer_id,
    c.order_count as customer_order_count,
    o.order_count as orders_order_count
from {{ ref('customers') }} c
join (
    select customer_id, count(*) as order_count
    from {{ ref('orders') }}
    group by customer_id
) o on c.customer_id = o.customer_id
where c.order_count != o.order_count
