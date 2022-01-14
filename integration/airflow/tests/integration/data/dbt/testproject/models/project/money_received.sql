-- SPDX-License-Identifier: Apache-2.0

select
    users.user_id,
    transactions.currency,
    sum(transactions.amount) as amount
from {{ ref('users') }}
left join {{ ref('transactions') }}
on users.user_id=transactions.user_id
where leg='c'
group by user_id, currency
