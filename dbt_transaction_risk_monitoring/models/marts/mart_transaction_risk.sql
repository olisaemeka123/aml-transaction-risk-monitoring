with transactions as (

    select
        transaction_id,
        transaction_ts,
        sender_account_id,
        receiver_account_id,
        amount

    from {{ ref('stg_transactions') }}

),

-- Velocity signal: how many transactions an account made recently
velocity as (

    select
        transaction_id,
        account_id,
        txn_count_last_1h,
        txn_count_last_24h

    from {{ ref('int_account_velocity') }}

),

-- Amount signal: how unusual the amount is against the account's own history
amount_stats as (

    select
        transaction_id,
        account_id,
        prior_txn_count_30d,
        amount_z_score

    from {{ ref('int_account_amount_stats') }}

),

-- Jurisdiction signal: whether an account is based in a high-risk country
jurisdiction as (

    select
        account_id,
        is_high_risk_country

    from {{ ref('int_account_jurisdiction') }}

),

-- Join all signals to each transaction, using the sender's velocity and stats
signals as (

    select
        t.transaction_id,
        t.transaction_ts,
        t.sender_account_id,
        t.receiver_account_id,
        t.amount,
        v.txn_count_last_1h,
        v.txn_count_last_24h,
        s.prior_txn_count_30d,
        s.amount_z_score,

        -- True if either the sender or the receiver is in a high-risk country
        coalesce(
            sj.is_high_risk_country or rj.is_high_risk_country,
            false
        ) as involves_high_risk_country

    from transactions as t
    left join velocity as v
        on
            t.transaction_id = v.transaction_id
            and t.sender_account_id = v.account_id
    left join amount_stats as s
        on
            t.transaction_id = s.transaction_id
            and t.sender_account_id = s.account_id
    left join jurisdiction as sj
        on t.sender_account_id = sj.account_id
    left join jurisdiction as rj
        on t.receiver_account_id = rj.account_id
),

-- Apply the detection rules; thresholds come from vars in dbt_project.yml
hits as (

    select
        *,

        -- Rule 1: burst of activity in the last hour
        if(
            txn_count_last_1h >= {{ var('velocity_1h_threshold') }}, 1, 0
        ) as hit_velocity,

        -- Rule 2: unusually large amount, only for accounts with enough history
        if(
            prior_txn_count_30d >= {{ var('min_prior_txns') }}
            and amount_z_score >= {{ var('amount_z_threshold') }},
            1,
            0
        ) as hit_amount_outlier,

        -- Rule 3: high-risk country plus a moderately unusual amount
        if(
            involves_high_risk_country
            and prior_txn_count_30d >= {{ var('min_prior_txns') }}
            and amount_z_score >= {{ var('jurisdiction_z_threshold') }},
            1,
            0
        ) as hit_high_risk_amount

    from signals

)

-- Final output: add the rule count and the overall flag
select
    *,
    hit_velocity + hit_amount_outlier + hit_high_risk_amount as rules_triggered,
    hit_velocity + hit_amount_outlier + hit_high_risk_amount > 0 as is_flagged

from hits
