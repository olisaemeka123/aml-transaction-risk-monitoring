with velocity_count as (

    select
        account_id,
        transaction_id,
        transaction_ts,
        amount,
        account_role,

        -- unix_seconds() avoids BigQuery's TIMESTAMP-in-RANGE restriction
        count(*) over (
            partition by account_id
            order by unix_seconds(transaction_ts)
            range between 3600 preceding and current row  -- 1 hour, in seconds
        ) as txn_count_last_1h,

        count(*) over (
            partition by account_id
            order by unix_seconds(transaction_ts)
            -- 24 hours, in seconds
            range between 86400 preceding and current row
        ) as txn_count_last_24h,

        count(*) over (
            partition by account_id
            order by unix_seconds(transaction_ts)
            -- 7 days, in seconds
            range between 604800 preceding and current row
        ) as txn_count_last_7d

    from {{ ref('int_account_activity') }}

)

select * from velocity_count
