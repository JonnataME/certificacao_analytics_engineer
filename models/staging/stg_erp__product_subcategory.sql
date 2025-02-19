with
    product_subcategory as (
        select
            cast(productsubcategoryid as int) as product_subcategory_id
            , cast(productcategoryid as int) as product_category_id
            , cast(name as varchar) as product_subcategory_name
        from {{ source('erp', 'productsubcategory') }}
    )

    , final as (

        select
            {{ dbt_utils.generate_surrogate_key(['product_subcategory_id']) }} as product_subcategory_uid
            , {{ dbt_utils.generate_surrogate_key(['product_category_id']) }} as product_category_uid
            , product_subcategory_id
            , product_subcategory_name
        from product_subcategory
    )

select *
from final
