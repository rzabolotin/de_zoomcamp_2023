SELECT 
    tripid
FROM {{ref("stg_yellow_tripdata")}}
WHERE passenger_count < 0