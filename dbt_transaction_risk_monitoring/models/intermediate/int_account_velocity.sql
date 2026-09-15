with velocity_count as (

select
    account_id,
    transaction_id,
    transaction_ts,
    amount,
    role,
 
 -- converting timestamp field to numeric using unix seconds() to allow for range-based windowing, since big query doesn't allow use of timestamp in order by clause in window functions.
     count(*) over (
        partition by account_id
        order by unix_seconds(transaction_ts)
        range between 3600 preceding and current row  -- 1 hour, in seconds
    ) as txn_count_last_1h,
 
    count(*) over (
        partition by account_id
        order by unix_seconds(transaction_ts)
        range between 86400 preceding and current row  -- 24 hours, in seconds
    ) as txn_count_last_24h,
 
    count(*) over (
        partition by account_id
        order by unix_seconds(transaction_ts)
        range between 604800 preceding and current row  -- 7 days, in seconds
    ) as txn_count_last_7d

from {{ref('int_account_activity')}}

)

select * from velocity_count