with amount_stats as (

    select
        account_id,
        transaction_id,
        transaction_ts,
        amount,
        account_role,

        count(*) over (
            partition by account_id
            order by unix_seconds(transaction_ts)
            range between 2592000 preceding and 1 preceding  -- 30d in seconds
        ) as prior_txn_count_30d,

        avg(amount) over (
            partition by account_id
            order by unix_seconds(transaction_ts)
            range between 2592000 preceding and 1 preceding
        ) as avg_amount_30d,

        stddev_samp(amount) over (
            partition by account_id
            order by unix_seconds(transaction_ts)
            range between 2592000 preceding and 1 preceding
        ) as stddev_amount_30d

    from {{ ref('int_account_activity') }}

)

select
    account_id,
    transaction_id,
    transaction_ts,
    amount,
    account_role,
    prior_txn_count_30d,
    avg_amount_30d,
    stddev_amount_30d,
    safe_divide(amount - avg_amount_30d, stddev_amount_30d) as amount_z_score

from amount_stats
