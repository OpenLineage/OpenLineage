-- Leaf model: its failing test mustn't skip anything downstream.
select distinct status from {{ ref('stg_orders') }}
