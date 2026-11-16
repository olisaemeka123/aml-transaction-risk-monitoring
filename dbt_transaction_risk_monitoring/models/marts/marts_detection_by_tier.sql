with scored as (

    select
        r.rules_triggered,
        l.is_fraud_ground_truth

    from {{ ref('mart_transaction_risk') }} as r
    inner join {{ ref('stg_transaction_labels') }} as l
        on r.transaction_id = l.transaction_id

)

-- Fraud share per tier shows whether more rules means better alerts
select
    rules_triggered,
    count(*) as txn_count,
    countif(is_fraud_ground_truth) as fraud_count,
    safe_divide(countif(is_fraud_ground_truth), count(*)) as fraud_share

from scored
group by rules_triggered
