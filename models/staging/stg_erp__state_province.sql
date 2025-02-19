with
    state_province as (
        select
            cast(stateprovinceid as int) as state_province_id
            , cast(territoryid as int) as territory_id
            , cast(countryregioncode as varchar) as country_region_code
            , cast(stateprovincecode as varchar) as state_province_code
            , cast(name as varchar) as state_province_name
        from {{ source('erp', 'stateprovince') }}
    )

    , final as (
        select
            {{ dbt_utils.generate_surrogate_key(['state_province_id']) }} as state_province_uid
            , {{ dbt_utils.generate_surrogate_key(['territory_id']) }} as territory_uid
            , {{ dbt_utils.generate_surrogate_key(['country_region_code']) }} as country_region_uid
            , state_province_id
            , state_province_code
            , state_province_name
        from state_province
            
    )

select *
from final
