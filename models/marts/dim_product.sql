with
    product as (
        select * from {{ ref('stg_erp__product') }}
    ),
    product_category as (
        select * from {{ ref('stg_erp__product_category') }}
    ),
    product_subcategory as (
        select * from {{ ref('stg_erp__product_subcategory') }}
    ),
    product_model as (
        select * from {{ ref('stg_erp__product_model') }}
    ),
    enrich_product as (
        select
            product.product_uid
            , product.product_id
            , product.product_name
            , coalesce(product.product_line, 'not_filled') as product_line
            , coalesce(product_category.product_category_name, 'not_filled') as product_category_name
            , coalesce(product_subcategory.product_subcategory_name, 'not_filled') as product_subcategory_name
            , coalesce(product_model.product_model_name, 'not_filled') as product_model_name
        from product
        left join product_subcategory on product.product_subcategory_uid = product_subcategory.product_subcategory_uid
        left join product_category on product_subcategory.product_category_uid = product_category.product_category_uid
        left join product_model on product.product_model_uid = product_model.product_model_uid
    )
select *
from enrich_product
