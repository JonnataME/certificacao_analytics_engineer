with
    address as (
        select * from {{ ref('stg_erp__address') }}
    ),
    state_province as (
        select * from {{ ref('stg_erp__state_province') }}
    ),
    country_region as (
        select * from {{ ref('stg_erp__country_region') }}
    ),
    final as (
        select
            address.address_uid
            , address.address_id
            , address.postal_code
            , state_province.state_province_code
            , state_province.state_province_name
            , address.city_name
        from address
        left join state_province on address.state_province_uid = state_province.state_province_uid
        left join country_region on state_province.country_region_uid = country_region.country_region_uid
    )
select *
from final
