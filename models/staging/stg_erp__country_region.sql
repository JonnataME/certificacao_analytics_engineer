with
    country_region as (  
        select  
            cast(countryregioncode as varchar) as country_region_code
            , cast(name as varchar) as country_region_name
        from {{ source('erp', 'countryregion') }}  
    )
    
    , final as (
        select 
            {{ dbt_utils.generate_surrogate_key(['country_region_code']) }} as country_region_uid
           , country_region_code
           , country_region_name
        from country_region
    )

select *  
from final
