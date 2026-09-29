-- Units are set in the ETL DAG: temperature in °F, precipitation in inches, wind in mph.
-- Sunshine and daylight arrive in seconds and are converted to hours here.

with source as (

    select * from {{ source('raw', 'weather_daily') }}

)

select
    city,
    date as weather_date,
    latitude,
    longitude,
    temperature_2m_max,
    temperature_2m_min,
    temperature_2m_mean,
    apparent_temperature_max,
    precipitation_sum,
    precipitation_hours,
    precipitation_probability_max,
    wind_speed_10m_max,
    round(sunshine_duration / 3600, 2) as sunshine_hours,
    round(daylight_duration / 3600, 2) as daylight_hours,
    uv_index_max,
    weather_code,
    retrieved_at
from source
