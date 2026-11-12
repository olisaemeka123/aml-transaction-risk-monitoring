with accounts as (

    select
        account_id,
        country

    from {{ ref('stg_accounts') }}

),

jurisdictions as (

    select
        country_code,
        is_high_risk

    from {{ ref('jurisdictions') }}

)

select
    accounts.account_id,
    accounts.country,
    jurisdictions.is_high_risk as is_high_risk_country

from accounts
left join jurisdictions
    on accounts.country = jurisdictions.country_code
