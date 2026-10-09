with source as (

    select *
    from {{ source('raw_transaction_risk', 'transactions') }}

),

renamed as (

    select
        transaction_id,
        timestamp as transaction_ts,
        sender_account_id,
        receiver_account_id,
        amount,
        _dlt_load_id as dlt_load_id

    from source

)

select * from renamed
