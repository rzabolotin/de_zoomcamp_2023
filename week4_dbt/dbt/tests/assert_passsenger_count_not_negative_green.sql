SELECT 
    tripid
FROM {{ref("stg_green_tripdata")}}
WHERE passenger_count < 0