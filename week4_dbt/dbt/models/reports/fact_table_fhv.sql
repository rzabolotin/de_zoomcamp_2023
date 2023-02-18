{{ config(materialized='table') }}

WITH dim_zones as (
    select * from {{ ref('dim-zones') }}
    where borough != 'Unknown'
)
select 
    stg_table.tripid, 
    stg_table.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    stg_table.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    stg_table.pickup_datetime, 
    stg_table.dropoff_datetime, 
    stg_table.disp_base, 
    stg_table.affil_base, 
    stg_table.sr_flag

from {{ref("stg_fhv_tripdata")}} as stg_table
inner join dim_zones as pickup_zone
on stg_table.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on stg_table.dropoff_locationid = dropoff_zone.locationid