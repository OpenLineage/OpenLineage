select
    ifnull(money_received.user_id, money_send.user_id) as user_id,
    ifnull(money_received.currency, money_send.currency) as currency,
    ifnull(money_received.amount, 0) - ifnull(money_send.amount, 0) as balance
from {{ ref('money_received') }}
full outer join {{ ref('money_send') }}
on money_received.user_id=money_send.user_id
and money_received.currency=money_send.currency
