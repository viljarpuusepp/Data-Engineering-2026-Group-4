{{ config(materialized='table', engine='MergeTree()', order_by='rider_key') }}

select distinct
    cityHash64(concat(rideable_type, '|', member_casual)) as rider_key,
    rideable_type,
    member_casual
from {{ ref('stg_trips') }}

