{{ config(
    materialized='table',
    owner = 'yara_burvin',
    team = 'finance',
    
) }}

with 

fund_data as (
    select 
        fund_name,
        date,
        nav as fund_nav
    from {{ ref('fund_nav') }}
),

company_data as (
    select 
       *
    from {{ ref('int_company_data_clean') }}
),


company_aggregation as (
    select 
        company_name,
        fund_name,
        transaction_date,
        sum(transaction_amount) as company_valuation_in_fund
    from company_data
    group by company_name, fund_name, transaction_date
),


-- calculate the ownership percentage of each company in the fund based on the NAV
ownership_calc as (
    select 
        company_aggregation.company_name,
        company_aggregation.fund_name,
        company_aggregation.transaction_date as company_valuation_date,
        company_aggregation.company_valuation_in_fund,
        fund_data.fund_nav,
        fund_data.fund_nav / company_aggregation.company_valuation_in_fund as company_ownership_percentage
    from company_aggregation
    inner join fund_data 
        on company_aggregation.fund_name = fund_data.fund_name and company_aggregation.transaction_date = fund_data.date
),


calculate_fund_nav as (
    select 
        company_data.company_name,
        company_data.transaction_date as company_valuation_date,
        company_data.transaction_amount as company_valuation_in_fund,
        ownership_calc.company_ownership_percentage,
        company_data.transaction_amount  * ownership_calc.company_ownership_percentage as fund_nav_in_company
    from company_data
    inner join ownership_calc
        on company_data.fund_name = ownership_calc.fund_name
        and company_data.transaction_date = ownership_calc.company_valuation_date
)

select
    {{ dbt_utils.generate_surrogate_key(['company_name', 'company_valuation_date']) }} as surrogate_key,
    company_name,
    company_valuation_date,
    sum(fund_nav_in_company) as company_nav
from calculate_fund_nav
group by all