{{ config(materialized="table") }}
with
    nyc_weather as (
        select
            v:time::timestamp as observation_time,
            v:city.id::int as city_id,
            v:city.name::string as city_name,
            v:city.country::string as country,
            v:city.coord.lat::float as city_lat,
            v:city.coord.lon::float as city_lon,
            v:clouds.all::int as clouds,
            (v:main.temp::float) - 273.15 as temp_avg,
            (v:main.temp_min::float) - 273.15 as temp_min,
            (v:main.temp_max::float) - 273.15 as temp_max,
            v:weather[0].main::string as weather,
            v:weather[0].description::string as weather_desc,
            v:weather[0].icon::string as weather_icon,
            v:wind.deg::float as wind_dir,
            v:wind.speed::float as wind_speed
        from raw.weather.weather_data
        where city_id = 5128638

    )
select w.weather as conditions, count(t.*) as num_trips
from raw.citibike.trips as t
left outer join
    nyc_weather as w
    on date_trunc('hour', w.observation_time) = date_trunc('hour', t.starttime)
where conditions is not null
group by 1
order by 2 desc