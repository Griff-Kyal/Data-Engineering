SELECT 
    pickup_day, 
    SUM((total_amount)::numeric) AS revenue
FROM trip_amount
GROUP BY pickup_day
ORDER BY pickup_day;

SELECT 
    pickup_day, 
    COUNT(airport_fee) AS total_airport_trips
FROM airport_trips
GROUP BY pickup_day
ORDER BY pickup_day;

SELECT 
    pickup_day, 
    SUM((trip_distance_miles)::numeric) AS daily_mileage
FROM trip_mileage
GROUP BY pickup_day
ORDER BY pickup_day;

CREATE TABLE daily_summary AS
WITH revenue_cte AS (
    SELECT 
        pickup_day, 
        SUM(total_amount::numeric) AS revenue
    FROM trip_amount
    GROUP BY pickup_day
),
airport_cte AS (
    SELECT 
        pickup_day, 
        COUNT(airport_fee) AS total_airport_trips
    FROM airport_trips
    GROUP BY pickup_day
),
mileage_cte AS (
    SELECT 
        pickup_day, 
        SUM(trip_distance_miles::numeric) AS daily_mileage
    FROM trip_mileage
    GROUP BY pickup_day
)
SELECT 
    r.pickup_day,
    r.revenue,
    a.total_airport_trips,
    m.daily_mileage
FROM revenue_cte r
FULL OUTER JOIN airport_cte a ON r.pickup_day = a.pickup_day
FULL OUTER JOIN mileage_cte m ON COALESCE(r.pickup_day, a.pickup_day) = m.pickup_day
ORDER BY r.pickup_day NULLS LAST;