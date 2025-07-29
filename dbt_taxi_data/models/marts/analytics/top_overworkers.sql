{{
  config(
    tags = ['analytics']
  )
}}

    -- materialized = 'table',

WITH trip_sequences AS (
  SELECT 
    taxi_id,
    trip_start_timestamp,
    trip_end_timestamp,
    LAG(trip_end_timestamp) OVER (PARTITION BY taxi_id ORDER BY trip_start_timestamp) AS prev_trip_end,
    TIMESTAMP_DIFF(
      trip_start_timestamp, 
      LAG(trip_end_timestamp) OVER (PARTITION BY taxi_id ORDER BY trip_start_timestamp), 
      HOUR
    ) AS hours_since_last_trip
  FROM 
    {{ ref('stg_taxi_trips') }}
--   WHERE 
    -- trip_start_timestamp  is not null
    -- trip_start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 MONTH)
),

shift_analysis AS (
  SELECT 
    taxi_id,
    DATE(trip_start_timestamp) AS work_date,
    MIN(trip_start_timestamp) AS shift_start,
    MAX(trip_end_timestamp) AS shift_end,
    TIMESTAMP_DIFF(MAX(trip_end_timestamp), MIN(trip_start_timestamp), HOUR) AS shift_hours,
    SUM(CASE WHEN hours_since_last_trip < 8 THEN 1 ELSE 0 END) AS insufficient_breaks,
    COUNT(*) AS trips_in_shift
  FROM 
    trip_sequences
  GROUP BY 
    taxi_id, DATE(trip_start_timestamp)

)

SELECT 
  taxi_id,
  COUNT(*) AS total_shifts,
  AVG(shift_hours) AS avg_shift_hours,
  SUM(insufficient_breaks) AS total_insufficient_breaks,
  SUM(shift_hours) AS total_hours_worked,
  RANK() OVER (ORDER BY SUM(shift_hours) DESC) AS overworker_rank
FROM 
  shift_analysis
WHERE 
  shift_hours > 8
  AND insufficient_breaks > 0
GROUP BY 
  taxi_id
QUALIFY
  overworker_rank <= 100
ORDER BY 
  overworker_rank
