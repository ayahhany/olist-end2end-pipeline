{{ config(materialized='view') }}

SELECT 
    geolocation_zip_code_prefix, 
    geolocation_lat as latitude, 
    geolocation_lng as longitude, 
    geolocation_city as city, 
    geolocation_state as state,
    cast(updated_at_timestamp as timestamp) as updated_at_timestamp 

FROM 
    project_landing.geolocation_ayahany



 