with
    product as (
        select
            cast(productid as int) as product_id
            , cast(productmodelid as int) as product_model_id
            , cast(productsubcategoryid as int) as product_subcategory_id
            , cast(class as varchar) as product_class
            , cast(name as varchar) as product_name
            , cast(productline as varchar) as product_line
        from {{ source('erp', 'product') }}
    )

    , final as (
        select 
            {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_uid
            , {{ dbt_utils.generate_surrogate_key(['product_model_id']) }} as product_model_uid
            , {{ dbt_utils.generate_surrogate_key(['product_subcategory_id']) }} as product_subcategory_uid
            , product_id
            , product_class
            , product_name
            , product_line
        from product
    )

select *  
from final
