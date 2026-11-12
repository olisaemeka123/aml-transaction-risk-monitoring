with source as (

    select *
    from {{ source('raw_transaction_risk', 'accounts') }}

),

renamed as (

    select
        account_id,
        country

    from source

)

select * from renamed
