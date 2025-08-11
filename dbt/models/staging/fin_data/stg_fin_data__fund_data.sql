{{ config(
    materialized='view'
    
) }}

with source as (
    select * from {{ source('fin_data', 'fund_data') }}
)

select 
    fund_name,
    fund_size,
    transaction_type,
    transaction_index,
    transaction_date,
    transaction_amount,
    sector,
    country, -- ideally would like to have unified format for country too. But this should be fixed in the source data. We not use this downstream in any exercise
    region
from source 