with table1 as (
  select distinct channelGrouping, date, geoNetwork_country,
  SUM(totals_transactions) as Transactions
  from `data-to-insights.ecommerce.rev_transactions`
  GROUP BY 1,2,3
  order by date, geoNetwork_country asc
)

select channelGrouping as Channel, ARRAY_AGG(date) as Date, ARRAY_AGG(geoNetwork_country) as Country, array_agg(Transactions) as Transactions
from table1 group by channelGrouping
