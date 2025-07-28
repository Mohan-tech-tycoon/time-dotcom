{{
  config(
    materialized = 'view',
    tags = ['staging']
  )
}}

SELECT 
  *,
  TIMESTAMP_DIFF(trip_end_timestamp, trip_start_timestamp, MINUTE) AS trip_duration_minutes,
  EXTRACT(HOUR FROM trip_start_timestamp) AS pickup_hour,
  FORMAT_DATE('%A', DATE(trip_start_timestamp)) AS pickup_day_name
FROM 
  `gcp-devspace-coder.taxi_trips_dataset.raw_taxi_trips_data`
where trip_start_timestamp is not null