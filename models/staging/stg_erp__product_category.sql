with
    product_category as (
        select
            cast(productcategoryid as int) as product_category_id
            , cast(name as varchar) as product_category_name
        from {{ source('erp', 'productcategory') }}
    )

    , final as (
        select
            {{ dbt_utils.generate_surrogate_key(['product_category_id']) }} as product_category_uid
            , product_category_id
            , product_category_name
        from product_category
    )

select *  
from final
