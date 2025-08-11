{{ config(
    materialized='table',
    owner = 'yara_burvin',
    team = 'finance',
    
) }}

with 

fund_data as (
    select 
        *
    from {{ ref('int_fund_data_clean') }}
),

company_data as (
    select 
       *
    from {{ ref('int_company_data_clean') }}
),


-- assuming that commitment is always made before the first valuation.
first_valuation as (
    select 
        fund_name,
        min(transaction_date) as first_valuation_date,
    from fund_data
    where transaction_type = 'VALUATION'
    group by fund_name
),

-- Calculate fund ownership percentages.
commitment_calculation as (
    select 
        fund_data.fund_name,
        fund_data.fund_size,
        first_valuation.first_valuation_date,
        sum(iff(fund_data.transaction_type = 'COMMITMENT' and fund_data.transaction_date <= first_valuation.first_valuation_date, fund_data.transaction_amount, 0)) as total_commitment_amount,
    from fund_data
    left join first_valuation
        on fund_data.fund_name = first_valuation.fund_name
    where fund_data.transaction_type in ('COMMITMENT')
    group by all
),

-- Apply ownership scaling to company valuations.
cvc_ownership as (
    select 
        company_data.company_name,
        company_data.fund_name,
        company_data.transaction_date,
        commitment_calculation.fund_size,
        commitment_calculation.total_commitment_amount,
        commitment_calculation.total_commitment_amount / commitment_calculation.fund_size as ownership_percentage,
        company_data.transaction_amount,
        company_data.transaction_amount * ownership_percentage as fund_nav_in_company
    from company_data
    left join commitment_calculation
        on company_data.fund_name = commitment_calculation.fund_name

)

-- sum the NAV for each company across all funds.
select 
    {{ dbt_utils.generate_surrogate_key(['company_name', 'transaction_date']) }} as surrogate_key,
    company_name,
    transaction_date as date,
    sum(fund_nav_in_company) as company_nav
from cvc_ownership
group by all

