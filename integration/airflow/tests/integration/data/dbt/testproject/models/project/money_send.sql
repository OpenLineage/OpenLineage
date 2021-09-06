select
    users.user_id,
    transactions.currency,
    sum(transactions.amount) as amount
from {{ ref('users') }}
left join {{ ref('transactions') }}
on users.user_id=transactions.user_id
where leg='b'
group by user_id, currency
