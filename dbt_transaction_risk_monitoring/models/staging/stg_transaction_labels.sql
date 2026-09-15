with source as (

    select *
    from {{ source('raw_transaction_risk', 'transaction_labels') }}

),

renamed as (

    select
        transaction_id,
        is_fraud_ground_truth,
        sequence as label_sequence,
        fraud_pattern

    from source

)

select * from renamed
