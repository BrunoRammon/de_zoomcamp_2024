{{
    config(
        materialized='view'
    )
}}

select
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(Affiliated_base_number as string) as affiliated_base_number,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    {{ dbt.safe_cast("SR_Flag", api.Column.translate_type("integer")) }} as sr_flag,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
from {{ source('staging','fhv_tripdata') }}
-- where pickup_datetime >= "2019-01-01" and pickup_datetime < "2020-01-01"

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}