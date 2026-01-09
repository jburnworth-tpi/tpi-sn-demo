{{ config(
    materialized='table',
    table_format='iceberg',
    external_volume='EXVOL_LAKE',
    base_location_subpath='dbt_iceberg'
) }}

select *
from {{ ref('int_lrs_flattened') }}