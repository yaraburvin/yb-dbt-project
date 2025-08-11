{{ config(
    materialized='table',
    owner = 'yara_burvin',
    team = 'finance',
    
) }}

with cleanup as (
    select 
        fund_name,
        replace(replace(fund_size, ',', ''), ' ', '')::number as fund_size,
        upper(transaction_type) as transaction_type,
        transaction_index::number as transaction_index,
        to_date(transaction_date, 'DD/MM/YYYY') as transaction_date,
        replace(replace(transaction_amount, ',', ''), ' ', '')::number as transaction_amount,
        sector,
        country, -- ideally would like to have unified format for country too. But this should be fixed in the source data. We not use this downstream in any exercise
        region
    from {{ ref('stg_fin_data__fund_data') }}
)

select * from cleanup
