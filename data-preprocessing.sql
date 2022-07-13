# Split airport name from city_name, state_code
#
CREATE TABLE `argolis-01.argolis_01_tests.airports_1` AS 
WITH q3 AS
(
  WITH q2 AS 
  (
    WITH q1 AS 
    (
      SELECT airport as airport_code, SPLIT(airport_name,": ") AS airport_split
      FROM
        `argolis-01.argolis_01_tests.airport_delays` 
    )
    SELECT
      airport_code,
      airport_split[ORDINAL(2)] airport_name,
      airport_split[ORDINAL(1)] airport_coverage
      FROM q1, UNNEST(airport_split) AS split 
    )
  SELECT DISTINCT * FROM q2 ORDER BY airport_name
)
SELECT * FROM q3

# Split city_name and state_code
#
CREATE TABLE `argolis-01.argolis_01_tests.airports_2` AS 
WITH q2 AS 
(
  WITH
    q1 AS 
  (
    SELECT
      airport_code, airport_name,
      SPLIT(airport_coverage, ', ') AS airport_coverage_split
    FROM
      `argolis-01.argolis_01_tests.airports_1` 
  )
  SELECT DISTINCT
    airport_code,
    airport_name,
    airport_coverage_split[ORDINAL(1)] AS city_name,
    airport_coverage_split[ORDINAL(2)] AS state_code,
  FROM
    q1, UNNEST(airport_coverage_split)
)
SELECT DISTINCT * FROM q2 ORDER by 1

# Join states with climate regions
#
CREATE OR REPLACE TABLE `argolis-01.argolis_01_tests.us_states` AS
  SELECT s.*, r.string_field_1 AS region_name
  FROM 
    `argolis-01.argolis_01_tests.us_states_0` s
  LEFT OUTER JOIN
    `argolis-01.argolis_01_tests.us_climate_regions` r
  ON
    s.state_name = r.string_field_0

# Create table with Us_airports, city_name, state_code and region_name
#
CREATE TABLE `argolis-01.argolis_01_tests.us_airports` AS 
  SELECT a.airport_code, a.airport_name, a.city_name, a.state_code, s.region_name
  FROM `argolis-01.argolis_01_tests.airports_2` a
  LEFT OUTER JOIN `argolis-01.argolis_01_tests.us_states` s
  ON a.state_code = s.state_code
  WHERE state_name IS NOT NULL
  
# Join us_airports table with airport_delays
#
DROP TABLE `argolis-01.argolis_01_tests.us_airports_delays`  
CREATE TABLE `argolis-01.argolis_01_tests.us_airports_delays` 
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2012, 2023)) 
CLUSTER BY year, month
AS
SELECT a.year, a.month, a.carrier, a.carrier_name, b.airport_code, b.airport_name, b.city_name, b.state_code, b.region_name, a.arr_flights, a.arr_del15, a.carrier_ct, a.weather_ct, a.nas_ct, a.security_ct, a.late_aircraft_ct, a.arr_cancelled, a.arr_diverted, a.arr_delay, a.carrier_delay, a.weather_delay, a.nas_delay, a.security_delay, a.late_aircraft_delay
FROM `argolis-01.argolis_01_tests.us_airports_delays_raw` a 
INNER JOIN `argolis-01.argolis_01_tests.us_airports` b 
ON a.airport = b.airport_code

# Create summary of airline delays
#
CREATE OR REPLACE TABLE `argolis_01_tests.us_airlines_delays_summary` AS
WITH q1 AS 
(
  SELECT carrier_name, 
  SUM(arr_flights) AS total_flights,
  CAST(SUM(carrier_ct + weather_ct + nas_ct + security_ct + security_ct + late_aircraft_ct) AS INT64) AS total_delays,
  SUM(arr_delay + carrier_delay + weather_delay + nas_delay + security_delay + late_aircraft_delay) AS total_delayed_min
  FROM `argolis_01_tests.us_airports_delays` 
  GROUP BY carrier_name
  ORDER BY carrier_name
)
SELECT *, ROUND((total_delays / total_flights * 100), 2) AS pct_delayed, 
ROUND((total_delayed_min / total_delays), 2) AS avg_delayed_min 
FROM q1

# Create summary of airport delays
#
CREATE OR REPLACE TABLE `argolis_01_tests.us_airports_delays_summary` AS
WITH q1 AS 
(
  SELECT airport_code, airport_name, city_name, state_code, region_name,
  SUM(arr_flights) AS total_flights,
  CAST(SUM(carrier_ct + weather_ct + nas_ct + security_ct + security_ct + late_aircraft_ct) AS INT64) AS total_delays,
  SUM(arr_delay + carrier_delay + weather_delay + nas_delay + security_delay + late_aircraft_delay) AS total_delayed_min
  FROM `argolis_01_tests.us_airports_delays` 
  GROUP BY airport_code, airport_name, city_name, state_code, region_name
  ORDER BY airport_name
)
SELECT *, 
ROUND((SAFE_DIVIDE(total_delays, total_flights) * 100), 2) AS pct_delayed,
ROUND(SAFE_DIVIDE(total_delayed_min, total_delays), 2) AS avg_delayed_min 
FROM q1

# Percentiles
#
WITH q1 AS
(
  SELECT APPROX_QUANTILES(total_flights, 100) percentiles 
  FROM `argolis-01.argolis_01_tests.us_airports_delays_summary`
)
SELECT 
  percentiles[offset(25)] AS pct_25, 
  percentiles[offset(50)] AS pct_50, 
  percentiles[offset(75)] AS pct_75
FROM q1

# Median
#
SELECT
  PERCENTILE_CONT(total_flights, 0.5) OVER() AS median
FROM 
  `argolis-01.argolis_01_tests.us_airports_delays_summary` 
LIMIT 1

# US airlines stats
#
SELECT * FROM `argolis-01.argolis_01_tests.us_airlines_delays_summary` ORDER BY pct_delayed ASC
SELECT * FROM `argolis-01.argolis_01_tests.us_airlines_delays_summary` ORDER BY avg_delayed_min ASC
SELECT * FROM `argolis-01.argolis_01_tests.us_airlines_delays_summary` ORDER BY total_flights DESC


# US airports stats (filter by flights greater than median)
#
SELECT * FROM `argolis-01.argolis_01_tests.us_airports_delays_summary`
WHERE total_flights >= 
(
  SELECT PERCENTILE_CONT(total_flights, 0.5) OVER() AS median 
  FROM `argolis-01.argolis_01_tests.us_airports_delays_summary` 
  LIMIT 1
)
ORDER BY pct_delayed ASC

SELECT * FROM `argolis-01.argolis_01_tests.us_airports_delays_summary` 
WHERE total_flights >= 
(
  SELECT PERCENTILE_CONT(total_flights, 0.5) OVER() AS median 
  FROM `argolis-01.argolis_01_tests.us_airports_delays_summary` 
  LIMIT 1
)
ORDER BY avg_delayed_min ASC

SELECT * FROM `argolis-01.argolis_01_tests.us_airports_delays_summary` 
WHERE total_flights >= 
(
  SELECT PERCENTILE_CONT(total_flights, 0.5) OVER() AS median 
  FROM `argolis-01.argolis_01_tests.us_airports_delays_summary` 
  LIMIT 1
)
ORDER BY total_flights DESC

# US airports stats (filter by flights greater than average)
#
SELECT * FROM `argolis-01.argolis_01_tests.us_airports_delays_summary`
WHERE total_flights >= 
(
  SELECT AVG(total_flights) avg_flights
  FROM `argolis-01.argolis_01_tests.us_airports_delays_summary` 
)
ORDER BY pct_delayed ASC

SELECT * FROM `argolis-01.argolis_01_tests.us_airports_delays_summary` 
WHERE total_flights >= 
(
  SELECT AVG(total_flights) avg_flights
  FROM `argolis-01.argolis_01_tests.us_airports_delays_summary` 
)
ORDER BY avg_delayed_min ASC

SELECT * FROM `argolis-01.argolis_01_tests.us_airports_delays_summary` 
WHERE total_flights >= 
(
  SELECT AVG(total_flights) avg_flights
  FROM `argolis-01.argolis_01_tests.us_airports_delays_summary` 
)
ORDER BY total_flights DESC