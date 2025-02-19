with
    sales_order_detail as (
        select *
        from {{ ref('stg_erp__sales_order_detail') }}
    ),
    sales_order_header as (
        select *
        from {{ ref('stg_erp__sales_order_header') }}
    ),
    joined as (
        select
            sales_order_detail.sales_order_detail_uid
            , sales_order_header.sales_order_uid
            , sales_order_detail.sales_order_detail_id
            , sales_order_header.sales_order_id
            , sales_order_header.territory_uid
            , sales_order_header.bill_to_address_uid
            , sales_order_header.ship_to_address_uid
            , sales_order_header.customer_uid
            , sales_order_header.credit_card_uid
            , sales_order_detail.product_uid
            , sales_order_header.order_date
            , sales_order_header.due_date
            , sales_order_header.ship_date
            , sales_order_header.online_order_flag
            , sales_order_header.status_name
            , sales_order_detail.order_qty
            , sales_order_detail.unit_price
            , sales_order_detail.unit_price_discount
            , (sales_order_detail.order_qty * (sales_order_detail.unit_price * (1 - sales_order_detail.unit_price_discount))) as total_value
        from sales_order_header
        inner join sales_order_detail on sales_order_header.sales_order_uid = sales_order_detail.sales_order_uid
    )
    select *
    from joined
    