{{ config(
    materialized='table',
    owner = 'yara_burvin',
    team = 'finance',
    
) }}

with fund_data as (
    select 
        fund_name,
        transaction_type,
        transaction_date,
        transaction_amount
    from {{ ref('int_fund_data_clean') }}
    where transaction_type in ('CALL', 'DISTRIBUTION', 'VALUATION')
),


-- based on the given output, when two evaluations happen on the same date, we take the max amount
max_amounts as (
    select 
        fund_name,
        transaction_type,
        transaction_date,
        max(transaction_amount) as transaction_amount
    from fund_data
    group by fund_name, transaction_type, transaction_date
),


-- now, we should have one evaluation per date. On the date where evaluation does not happen, so i.e. CALL happens, we take the last evaluation amount and sum it up with CALL

last_evaluation_value as (
    select
        fund_name,
        transaction_type,
        transaction_date,
        transaction_amount,
        iff(transaction_type = 'VALUATION', transaction_amount, null) as valuation_amount,
        -- we group everything into segments between valuation so that we can eventually calculate rolling NAV 
        sum(case when transaction_type = 'VALUATION' then 1 else 0 end) 
            over (partition by fund_name order by transaction_date asc rows between unbounded preceding and current row) 
        as valuation_group
    from max_amounts
),


 --- finally, we calculate NAV
calculate_nav as (
    select
        fund_name,
        transaction_date as date,
        transaction_type,
        transaction_amount,
        valuation_group,
        -- for each row, sum up all transaction amounts that come before the current row + current row, withing the same valuation group and fund 
        sum(transaction_amount) over (partition by fund_name, valuation_group order by date rows between unbounded preceding and current row) as cumulative_nav
    from last_evaluation_value
)

select 
    fund_name,
    date,
    cumulative_nav as nav
from calculate_nav