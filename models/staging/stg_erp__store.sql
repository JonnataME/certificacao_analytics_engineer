with
    store as (
        select
            cast(businessentityid as int) as store_id
            , cast(name as varchar) as store_name
        from {{ source('erp', 'store') }}  
    )

    , final as (
        select
            {{ dbt_utils.generate_surrogate_key(['store_id']) }} as store_uid
            , store_id
            , store_name
        from store
    )

select *
from final
