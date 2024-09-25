{{ config(materialized="table") }}

with
    monthly_trips as (

        select monthname(starttime) as "month", count(*) as "num trips"
        from raw.citibike.trips
        group by 1
        order by 2 desc

    )

select *
from monthly_trips
