{{
  config(
    tags = ['analytics']
  )
}}


    -- materialized = 'table',


WITH last_three_months AS (
  SELECT 
    taxi_id,
    SUM(tips) AS total_tips,
    COUNT(*) AS total_trips,
    SUM(tips) / COUNT(*) AS avg_tip_per_trip,
    AVG(tips) AS avg_tip_percentage
  FROM 
    {{ ref('stg_taxi_trips') }}
  WHERE --trip_start_timestamp is not null and 
    date(timestamp(trip_start_timestamp)) >= date_SUB((select date(timestamp(max(trip_start_timestamp))) from {{ ref('stg_taxi_trips') }} ), INTERVAL 3 MONTH)
  GROUP BY 
    taxi_id
--   HAVING
--     COUNT(*) > 10  -- Only consider drivers with at least 10 trips
)

SELECT 
  taxi_id,
  total_tips,
  total_trips,
  avg_tip_per_trip,
  avg_tip_percentage,
  RANK() OVER (ORDER BY total_tips DESC) AS tip_earner_rank
FROM 
  last_three_months
QUALIFY
  tip_earner_rank <= 100
ORDER BY 
  tip_earner_rank


-- WITH last_three_months AS (
--   SELECT 
--     taxi_id,
--     SUM(tips) AS total_tips,
--     COUNT(*) AS total_trips,
--     SUM(tips) / COUNT(*) AS avg_tip_per_trip,
--     AVG(tip_percentage) AS avg_tip_percentage
--   FROM 
--     {{ ref('stg_taxi_trips') }}
--   WHERE 
--     trip_start_timestamp >= TIMESTAMP_SUB(select max(trip_start_timestamp) from {{ref('stg_taxi_trips')}}, INTERVAL 3 MONTH)
--   GROUP BY 
--     taxi_id
--   HAVING
--     COUNT(*) > 10  -- Only consider drivers with at least 10 trips
-- )

-- SELECT 
--   taxi_id,
--   total_tips,
--   total_trips,
--   avg_tip_per_trip,
--   avg_tip_percentage,
--   RANK() OVER (ORDER BY total_tips DESC) AS tip_earner_rank
-- FROM 
--   last_three_months
-- QUALIFY
--   tip_earner_rank <= 100
-- ORDER BY 
--   tip_earner_rank