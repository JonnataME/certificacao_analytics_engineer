with
    product_model as (
        select
            cast(productmodelid as int) as product_model_id
            , cast(name as varchar) as product_model_name
        from {{ source('erp', 'productmodel') }}
    )

    , final as (
        select
            {{ dbt_utils.generate_surrogate_key(['product_model_id']) }} as product_model_uid
            , product_model_id
            , product_model_name
        from product_model
    )

select *  
from final
