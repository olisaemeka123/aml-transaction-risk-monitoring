with as_sender as (

select
    sender_account_id as account_id,
    transaction_id,
    transaction_ts,
    amount,
    'sender' as role

from {{ ref('stg_transactions') }}

),

as_receiver as(
    select
        receiver_account_id as account_id,
        transaction_id,
        transaction_ts,
        amount,
        'receiver' as role

    from {{ ref('stg_transactions') }}
)

select * from as_sender
union all
select * from as_receiver