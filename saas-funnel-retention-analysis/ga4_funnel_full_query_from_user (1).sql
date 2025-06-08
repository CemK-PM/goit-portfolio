
with event as (
  Select
    date(timestamp_micros(event_timestamp)) as event_date,
    event_name, 
    traffic_source.source,
    traffic_source.medium,
    (select value.string_value from e.event_params where key = 'campaign') as campaign,
    concat(user_pseudo_id, cast((select value.int_value from unnest(event_params) where key = 'ga_session_id') as string)) as user_session_id
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` as e
  Where
    _TABLE_SUFFIX BETWEEN '20210101' AND '20211231'
    and event_name in ('begin_checkout','add_to_cart','purchase')
),
user_session_count as (
  select
    event_date,
    traffic_source.source,
    traffic_source.medium,
    (select value.string_value from e.event_params where key = 'campaign') as campaign,
    count(distinct user_session_id) as user_sessions_count,
    count(distinct case when event_name = 'begin_checkout' then user_session_id end) as checkout_count,
    count(distinct case when event_name = 'add_to_cart' then user_session_id end) as atc_count,
    count(distinct case when event_name = 'purchase' then user_session_id end) as order_count
  from
    event
  group by 1,2,3,4
),
cr_rates as (
  select
    event_date,
    traffic_source.source,
    traffic_source.medium,
    campaign,
    user_sessions_count,
    checkout_count / user_sessions_count as co_cr,
    atc_count / user_sessions_count as atc_cr,
    order_count / user_sessions_count as order_cr
  from user_session_count
)
select 
  event_date,
  traffic_source.source,
  traffic_source.medium,
  campaign,
  user_sessions_count,
  co_cr,
  atc_cr,
  order_cr  
from cr_rates
group by 1,2,3,4,5,6,7,8;
