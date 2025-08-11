# Fund NAV Model

## Description
This model calculates fund-level Net Asset Value (NAV)

## Sample Data

| Fund Name | Valuation Date | Fund NAV |
|-----------|----------------|----------|
| Palisade Ridge Capital | 2021-03-31 | 9000000 |
| Summitvale Equity Group | 2021-06-30 | 12000000 |
| Summitvale Equity Group | 2021-07-01 | 11000000 |
| Ironbrook Capital Partners | 2021-06-30 | 70000000 |
| Summitvale Equity Group | 2021-12-31 | 13000000 |
| Palisade Ridge Capital | 2021-06-30 | 9800000 |
| Palisade Ridge Capital | 2021-07-15 | 10800000 |
| Palisade Ridge Capital | 2021-08-11 | 10500000 |
| Palisade Ridge Capital | 2020-12-31 | 8500000 |
| Ironbrook Capital Partners | 2020-12-31 | 50000000 |
| Palisade Ridge Capital | 2021-12-31 | 12000000 |
| Summitvale Equity Group | 2021-03-31 | 11000000 |
| Summitvale Equity Group | 2020-12-31 | 10000000 |
| Summitvale Equity Group | 2021-09-30 | 12000000 |
| Ironbrook Capital Partners | 2021-12-31 | 71000000 |
| Palisade Ridge Capital | 2021-09-30 | 11000000 |

## Data Notes

1. **Duplicate Handling**: When multiple evaluations occur on the same date, the **maximum amount** is taken.
2. the above can also be true if we are taking latest **index**, however confidence to this approach is low since index has some inconsistencies with data values.
3. **Valuation Grouping**: NAV is calculated in segments between valuations, with rolling calculations within each valuation period.


