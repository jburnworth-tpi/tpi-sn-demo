{{ config(
    materialized='table',
    table_format='iceberg',
    external_volume='EXVOL_LAKE',
    base_location_subpath='dbt_iceberg'
) }}

select *
from {{ ref('stg_ccmsi__lrs_raw_1g') }}