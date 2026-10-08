-- Calendar dimension generated in SQL (no package needed). Key = yyyymmdd integer.
with days as (
    select generate_series('2016-01-01'::date, '2026-12-31'::date, interval '1 day')::date as date_day
)
select
    to_char(date_day, 'YYYYMMDD')::int          as date_key,
    date_day,
    extract(isodow from date_day)::int          as day_of_week,      -- 1 = Monday
    trim(to_char(date_day, 'Day'))              as day_name,
    extract(isodow from date_day) in (6, 7)     as is_weekend,
    extract(day from date_day)::int             as day_of_month,
    extract(week from date_day)::int            as iso_week,
    extract(month from date_day)::int           as month,
    trim(to_char(date_day, 'Month'))            as month_name,
    to_char(date_day, 'YYYY-MM')                as year_month,
    extract(quarter from date_day)::int         as quarter,
    extract(year from date_day)::int            as year
from days
