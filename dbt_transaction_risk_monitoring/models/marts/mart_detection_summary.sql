with scored as (

    select
        r.is_flagged,
        l.is_fraud_ground_truth

    from {{ ref('mart_transaction_risk') }} as r
    inner join {{ ref('stg_transaction_labels') }} as l
        on r.transaction_id = l.transaction_id

),

-- Confusion matrix: fraud/normal against flagged/not flagged
counts as (

    select
        countif(is_fraud_ground_truth and is_flagged) as true_positives,
        countif(not is_fraud_ground_truth and is_flagged) as false_positives,
        countif(is_fraud_ground_truth and not is_flagged) as false_negatives,
        countif(not is_fraud_ground_truth and not is_flagged) as true_negatives

    from scored

)

-- Headline metrics for the whole rule set
select
    *,
    safe_divide(
        true_positives, true_positives + false_positives
    ) as precision_rate,
    safe_divide(
        true_positives, true_positives + false_negatives
    ) as recall_rate,
    safe_divide(
        false_positives, false_positives + true_negatives
    ) as false_positive_rate

from counts
