with
    address as (
        select
            cast(addressid as int) as address_id
            , cast(stateprovinceid as int) as state_province_id
            , cast(postalcode as varchar) as postal_code
            , cast(city as varchar) as city_name
        from {{ source('erp', 'address') }}  
    )
    
    , final as (
        select
            {{ dbt_utils.generate_surrogate_key(['address_id']) }} as address_uid
            , {{ dbt_utils.generate_surrogate_key(['state_province_id']) }} as state_province_uid
            , address_id
            , postal_code
            , city_name
        from address
    )

select *
from final
