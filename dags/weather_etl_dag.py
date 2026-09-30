import json
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

TARGET_TABLE = "RAW.WEATHER_DAILY"
SNOWFLAKE_CONN_ID = "weather_warehouse"
OPEN_METEO_CONN_ID = "open_meteo_api"

# Maps Open-Meteo's daily field names -> this table's column names
DAILY_FIELDS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "apparent_temperature_max",
    "precipitation_sum",
    "precipitation_hours",
    "precipitation_probability_max",
    "wind_speed_10m_max",
    "sunshine_duration",
    "daylight_duration",
    "uv_index_max",
    "weather_code",
]

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    CITY                          VARCHAR(50)     NOT NULL,
    DATE                          DATE            NOT NULL,
    LATITUDE                      NUMBER(9,6)     NOT NULL,
    LONGITUDE                     NUMBER(9,6)     NOT NULL,
    TEMPERATURE_2M_MAX            NUMBER(5,1),
    TEMPERATURE_2M_MIN            NUMBER(5,1),
    TEMPERATURE_2M_MEAN           NUMBER(5,1),
    APPARENT_TEMPERATURE_MAX      NUMBER(5,1),
    PRECIPITATION_SUM             NUMBER(6,3),
    PRECIPITATION_HOURS           NUMBER(4,1),
    PRECIPITATION_PROBABILITY_MAX NUMBER(3,0),
    WIND_SPEED_10M_MAX            NUMBER(5,1),
    SUNSHINE_DURATION             NUMBER(8,1),
    DAYLIGHT_DURATION             NUMBER(8,1),
    UV_INDEX_MAX                  NUMBER(4,2),
    WEATHER_CODE                  NUMBER(2,0),
    RETRIEVED_AT                  TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (CITY, DATE)
);
"""

MERGE_SQL = f"""
MERGE INTO {TARGET_TABLE} AS target
USING (SELECT
    %(city)s AS CITY, %(date)s AS DATE, %(latitude)s AS LATITUDE, %(longitude)s AS LONGITUDE,
    %(temperature_2m_max)s AS TEMPERATURE_2M_MAX, %(temperature_2m_min)s AS TEMPERATURE_2M_MIN,
    %(temperature_2m_mean)s AS TEMPERATURE_2M_MEAN, %(apparent_temperature_max)s AS APPARENT_TEMPERATURE_MAX,
    %(precipitation_sum)s AS PRECIPITATION_SUM, %(precipitation_hours)s AS PRECIPITATION_HOURS,
    %(precipitation_probability_max)s AS PRECIPITATION_PROBABILITY_MAX, %(wind_speed_10m_max)s AS WIND_SPEED_10M_MAX,
    %(sunshine_duration)s AS SUNSHINE_DURATION, %(daylight_duration)s AS DAYLIGHT_DURATION,
    %(uv_index_max)s AS UV_INDEX_MAX, %(weather_code)s AS WEATHER_CODE
) AS source
ON target.CITY = source.CITY AND target.DATE = source.DATE
WHEN MATCHED AND (
    target.LATITUDE IS DISTINCT FROM source.LATITUDE
    OR target.LONGITUDE IS DISTINCT FROM source.LONGITUDE
    OR target.TEMPERATURE_2M_MAX IS DISTINCT FROM source.TEMPERATURE_2M_MAX
    OR target.TEMPERATURE_2M_MIN IS DISTINCT FROM source.TEMPERATURE_2M_MIN
    OR target.TEMPERATURE_2M_MEAN IS DISTINCT FROM source.TEMPERATURE_2M_MEAN
    OR target.APPARENT_TEMPERATURE_MAX IS DISTINCT FROM source.APPARENT_TEMPERATURE_MAX
    OR target.PRECIPITATION_SUM IS DISTINCT FROM source.PRECIPITATION_SUM
    OR target.PRECIPITATION_HOURS IS DISTINCT FROM source.PRECIPITATION_HOURS
    OR target.PRECIPITATION_PROBABILITY_MAX IS DISTINCT FROM source.PRECIPITATION_PROBABILITY_MAX
    OR target.WIND_SPEED_10M_MAX IS DISTINCT FROM source.WIND_SPEED_10M_MAX
    OR target.SUNSHINE_DURATION IS DISTINCT FROM ROUND(source.SUNSHINE_DURATION, 1)
    OR target.DAYLIGHT_DURATION IS DISTINCT FROM ROUND(source.DAYLIGHT_DURATION, 1)
    OR target.UV_INDEX_MAX IS DISTINCT FROM source.UV_INDEX_MAX
    OR target.WEATHER_CODE IS DISTINCT FROM source.WEATHER_CODE
) THEN UPDATE SET
    LATITUDE = source.LATITUDE, LONGITUDE = source.LONGITUDE,
    TEMPERATURE_2M_MAX = source.TEMPERATURE_2M_MAX, TEMPERATURE_2M_MIN = source.TEMPERATURE_2M_MIN,
    TEMPERATURE_2M_MEAN = source.TEMPERATURE_2M_MEAN, APPARENT_TEMPERATURE_MAX = source.APPARENT_TEMPERATURE_MAX,
    PRECIPITATION_SUM = source.PRECIPITATION_SUM, PRECIPITATION_HOURS = source.PRECIPITATION_HOURS,
    PRECIPITATION_PROBABILITY_MAX = source.PRECIPITATION_PROBABILITY_MAX, WIND_SPEED_10M_MAX = source.WIND_SPEED_10M_MAX,
    SUNSHINE_DURATION = source.SUNSHINE_DURATION, DAYLIGHT_DURATION = source.DAYLIGHT_DURATION,
    UV_INDEX_MAX = source.UV_INDEX_MAX, WEATHER_CODE = source.WEATHER_CODE,
    RETRIEVED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
    (CITY, DATE, LATITUDE, LONGITUDE, TEMPERATURE_2M_MAX, TEMPERATURE_2M_MIN, TEMPERATURE_2M_MEAN,
     APPARENT_TEMPERATURE_MAX, PRECIPITATION_SUM, PRECIPITATION_HOURS, PRECIPITATION_PROBABILITY_MAX,
     WIND_SPEED_10M_MAX, SUNSHINE_DURATION, DAYLIGHT_DURATION, UV_INDEX_MAX, WEATHER_CODE)
VALUES
    (source.CITY, source.DATE, source.LATITUDE, source.LONGITUDE, source.TEMPERATURE_2M_MAX,
     source.TEMPERATURE_2M_MIN, source.TEMPERATURE_2M_MEAN, source.APPARENT_TEMPERATURE_MAX,
     source.PRECIPITATION_SUM, source.PRECIPITATION_HOURS, source.PRECIPITATION_PROBABILITY_MAX,
     source.WIND_SPEED_10M_MAX, source.SUNSHINE_DURATION, source.DAYLIGHT_DURATION,
     source.UV_INDEX_MAX, source.WEATHER_CODE);
"""


@task
def extract():
    """Pull past 60 days of daily weather for every city in the weather_cities Variable."""
    cities = json.loads(Variable.get("weather_cities"))

    hook = HttpHook(method="GET", http_conn_id=OPEN_METEO_CONN_ID)
    results = []

    for city in cities:
        params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "past_days": 60,
        "forecast_days": 0,
        "daily": ",".join(DAILY_FIELDS),
        "timezone": "auto",
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
        "wind_speed_unit": "mph",
    }
        response = hook.run(endpoint="/v1/forecast", data=params)
        response.raise_for_status()
        results.append({"city": city["name"], "data": response.json()})

    return results


@task
def transform(extracted: list):
    """Flatten each city's daily arrays into one row per (city, date)."""
    records = []

    for item in extracted:
        city = item["city"]
        payload = item["data"]
        latitude = payload["latitude"]
        longitude = payload["longitude"]
        daily = payload["daily"]

        for i in range(len(daily["time"])):
            record = {
                "city": city,
                "date": daily["time"][i],
                "latitude": latitude,
                "longitude": longitude,
            }
            for field in DAILY_FIELDS:
                record[field] = daily[field][i]
            records.append(record)

    return records


@task
def load(records: list):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute(CREATE_TABLE_SQL)
        cur.execute("BEGIN;")

        for record in records:
            cur.execute(MERGE_SQL, record)

        cur.execute("COMMIT;")
        print(f"Upserted {len(records)} records into {TARGET_TABLE}")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Load failed: {e}")
        raise RuntimeError(f"Load into {TARGET_TABLE} failed") from e

    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id="weather_etl_dag",
    start_date=datetime(2026, 9, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["weather", "snowflake", "etl"],
) as dag:

    extracted_data = extract()
    transformed_records = transform(extracted_data)
    load_task = load(transformed_records)

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="weather_dbt_dag",
    )

    load_task >> trigger_dbt