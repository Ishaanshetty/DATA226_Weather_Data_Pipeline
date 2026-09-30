-- Derived metrics per BRD Section 4. Three metrics are intentionally excluded here:
--   * Preference-based win rate: depends on a viewer's chosen thresholds, computed in the BI tool, not dbt.
--   * Forecast miss rate / Forecast drift: need forecast-vs-actual history, which requires the
--     snapshot (raw_weather_snapshot) to exist first. Add them once that's built.
--
-- Thresholds used below (adjust to taste):
--   Dry day:       precipitation_sum < 0.01 in
--   Comfortable:   65-80°F mean temp, < 0.01 in rain, wind under 15 mph
--   Weekend rain:  > 0.1 in precipitation on Sat/Sun
--   UV danger:     UV index >= 8

with base as (

    select * from {{ ref('stg_weather') }}

),

daily as (

    select
        *,
        lag(temperature_2m_mean) over (partition by city order by weather_date) as prev_day_mean_temp,
        dayofweek(weather_date) as day_of_week,  -- 0 = Sunday, 6 = Saturday
        case when precipitation_sum < 0.01 then 1 else 0 end as is_dry_day,
        case
            when temperature_2m_mean between 65 and 80
             and precipitation_sum < 0.01
             and wind_speed_10m_max < 15
            then 1 else 0
        end as is_comfortable_day
    from base

),

rolling as (

    select
        *,
        -- Weather whiplash index: rolling 7-day avg of |today's mean temp - yesterday's|
        avg(abs(temperature_2m_mean - prev_day_mean_temp)) over (
            partition by city order by weather_date
            rows between 6 preceding and current row
        ) as weather_whiplash_index,

        -- Rolling 7-day temperature average
        avg(temperature_2m_mean) over (
            partition by city order by weather_date
            rows between 6 preceding and current row
        ) as rolling_temp_avg_7d,

        -- Predictability score: coefficient of variation of temp over a 7-day window
        stddev(temperature_2m_mean) over (
            partition by city order by weather_date
            rows between 6 preceding and current row
        ) / nullif(avg(temperature_2m_mean) over (
            partition by city order by weather_date
            rows between 6 preceding and current row
        ), 0) as predictability_score,

        -- Rolling 7-day rainfall total
        sum(precipitation_sum) over (
            partition by city order by weather_date
            rows between 6 preceding and current row
        ) as rolling_rainfall_total_7d,

        -- Temperature anomaly vs. a trailing 30-day baseline
        temperature_2m_mean - avg(temperature_2m_mean) over (
            partition by city order by weather_date
            rows between 29 preceding and current row
        ) as temp_anomaly_vs_baseline,

        -- Gaps-and-islands group ids: increments each time the day breaks the streak
        sum(case when is_dry_day = 0 then 1 else 0 end) over (
            partition by city order by weather_date
            rows unbounded preceding
        ) as dry_spell_group,

        sum(case when is_comfortable_day = 0 then 1 else 0 end) over (
            partition by city order by weather_date
            rows unbounded preceding
        ) as comfort_streak_group

    from daily

),

streaks as (

    select
        *,
        case
            when is_dry_day = 1
            then row_number() over (partition by city, dry_spell_group, is_dry_day order by weather_date)
            else 0
        end as dry_spell_days,

        case
            when is_comfortable_day = 1
            then row_number() over (partition by city, comfort_streak_group, is_comfortable_day order by weather_date)
            else 0
        end as comfort_streak_length

    from rolling

),

final as (

    select
        city,
        weather_date,
        latitude,
        longitude,

        -- passthrough raw metrics, useful for the dashboard's base charts
        temperature_2m_max,
        temperature_2m_min,
        temperature_2m_mean,
        apparent_temperature_max,
        precipitation_sum,
        wind_speed_10m_max,
        uv_index_max,
        weather_code,

        -- derived metrics
        round(weather_whiplash_index, 2) as weather_whiplash_index,
        round(predictability_score, 3) as predictability_score,
        round(rolling_temp_avg_7d, 1) as rolling_temp_avg_7d,
        round(rolling_rainfall_total_7d, 3) as rolling_rainfall_total_7d,
        round(temp_anomaly_vs_baseline, 1) as temp_anomaly_vs_baseline,
        dry_spell_days,
        comfort_streak_length,
        (temperature_2m_max - temperature_2m_min > 20) as is_layer_up_day,
        round(apparent_temperature_max - temperature_2m_max, 1) as feels_like_gap,
        round(precipitation_sum / nullif(precipitation_hours, 0), 3) as rain_intensity,
        round(sunshine_hours / nullif(daylight_hours, 0), 3) as sunshine_ratio,
        (day_of_week in (0, 6) and precipitation_sum > 0.1) as is_weekend_rain_day,
        (uv_index_max >= 8) as is_uv_danger_day

    from streaks

)

select * from final
