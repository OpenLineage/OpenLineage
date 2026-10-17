select * from {{ ref('stg_orders') }} where amount <= 0
