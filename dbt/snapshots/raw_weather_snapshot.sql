{% snapshot raw_weather_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key="city || '-' || date",
        strategy='timestamp',
        updated_at='retrieved_at',
    )
}}

select * from {{ source('raw', 'weather_daily') }}

{% endsnapshot %}
