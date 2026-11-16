with scored as (

    select
        r.transaction_id,
        r.is_flagged,
        coalesce(l.fraud_pattern, 'normal') as fraud_pattern

    from {{ ref('mart_transaction_risk') }} as r
    inner join {{ ref('stg_transaction_labels') }} as l
        on r.transaction_id = l.transaction_id

)

-- For fraud patterns flag_rate is the catch rate (recall)
-- For the normal row it is the false-positive rate
select
    fraud_pattern,
    count(*) as txn_count,
    countif(is_flagged) as flagged_count,
    safe_divide(countif(is_flagged), count(*)) as flag_rate

from scored
group by fraud_pattern
