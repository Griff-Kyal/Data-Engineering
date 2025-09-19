CREATE TABLE trip_amount AS
SELECT
  trip_distance_miles,
  fare_amount,
  tip_amount,
  total_amount,
  date_trunc('day', pickup_datetime) AS pickup_day
FROM master_table_2025_07;

CREATE TABLE airport_trips AS
SELECT
  date_trunc('day', pickup_datetime) AS pickup_day,
  passenger_count,
  trip_distance_miles,
  fare_amount,
  tip_amount,
  total_amount,
  airport_fee
FROM master_table_2025_07
WHERE airport_fee > 0;

CREATE TABLE trip_mileage AS
SELECT
  date_trunc('day', pickup_datetime) AS pickup_day,
  passenger_count,
  "pickup_locID",
  "dropoff_locID",
  trip_distance_miles
FROM master_table_2025_07;

CREATE TABLE payment_types AS
SELECT
  pickup_datetime,
  fare_amount,
  tip_amount,
  total_amount,
  store_and_fwd_flag,
  payment_type
FROM master_table_2025_07;

CREATE TABLE trip_charges AS
SELECT
  pickup_datetime,
  fare_amount,
  mta_tax,
  tip_amount,
  extra,
  tolls_amount,
  improvement_surcharge,
  congestion_surcharge,
  airport_fee,
  cbd_congestion_fee,
  (fare_amount+mta_tax+tip_amount+extra+tolls_amount+improvement_surcharge+congestion_surcharge+airport_fee+cbd_congestion_fee) AS grand_total_fare
FROM master_table_2025_07