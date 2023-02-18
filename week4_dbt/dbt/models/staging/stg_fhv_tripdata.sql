{{ config(materialized='view') }}
 
select
   -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'Affiliated_base_number', 'pickup_datetime']) }} as tripid,
    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

   cast(dispatching_base_num as string) as disp_base,
   cast(Affiliated_base_number as string) as affil_base,
   
   cast(SR_Flag as numeric) as sr_flag
    
   
from {{ source('dataset_hw4','fhv_data') }}

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}