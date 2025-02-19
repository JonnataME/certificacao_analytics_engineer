with
    sales_order_header_sales_reason as (
        select
            cast(salesorderid as int) as sales_order_id
            , cast(salesreasonid as int) as sales_reason_id
        from {{ source('erp', 'salesorderheadersalesreason') }}  
    )

    , final as (
        select
            {{ dbt_utils.generate_surrogate_key(['sales_order_id']) }} as sales_order_uid
            , {{ dbt_utils.generate_surrogate_key(['sales_reason_id']) }} as sales_reason_uid
        from sales_order_header_sales_reason
    )

select *
from final