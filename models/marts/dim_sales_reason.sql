with
    sales_reason as (
        select * from {{ ref('stg_erp__sales_reason') }}
    ),
    sales_order_header_sales_reason as (
        select * from {{ ref('stg_erp__sales_order_header_sales_reason') }}
    ),
    sales_order_detail as (
        select * from {{ ref('stg_erp__sales_order_detail') }}
    ),
    final as (
        select
            sales_order_detail.sales_order_detail_uid
            , sales_reason.reason_type
            , sales_reason.reason_name
        from sales_reason
        inner join sales_order_header_sales_reason on sales_reason.sales_reason_uid = sales_order_header_sales_reason.sales_reason_uid
        inner join sales_order_detail on sales_order_header_sales_reason.sales_order_uid = sales_order_detail.sales_order_uid
    )
select *
from final
