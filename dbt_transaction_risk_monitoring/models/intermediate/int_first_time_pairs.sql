with ranked as (

    select
        transaction_id,
        transaction_ts,
        sender_account_id,
        receiver_account_id,

        row_number() over (
            partition by sender_account_id, receiver_account_id
            order by transaction_ts, transaction_id
        ) as pair_txn_sequence

    from {{ ref('stg_transactions') }}

)

select
    transaction_id,
    transaction_ts,
    sender_account_id,
    receiver_account_id,
    pair_txn_sequence,
    pair_txn_sequence = 1 as is_first_time_pair

from ranked
