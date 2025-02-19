with
    sales_order_header as (
        select
            cast(salesorderid as int) as sales_order_id
            , cast(territoryid as int) as territory_id
            , cast(billtoaddressid as int) as bill_to_address_id
            , cast(shiptoaddressid as int) as ship_to_address_id
            , cast(customerid as int) as customer_id
            , cast(creditcardid as int) as credit_card_id
            , cast(duedate as date) as due_date
            , cast(orderdate as date) as order_date
            , cast(shipdate as date) as ship_date
            , cast(onlineorderflag as int) as online_order_flag
            , cast(status as int) as status
            , cast(taxamt as decimal(20,2)) as taxa_mt
            , cast(subtotal as decimal(20,2)) as subtotal
            , cast(totaldue as decimal(20,2)) as total_due
        from {{ source('erp', 'salesorderheader') }}
    ),

    final as (
        select
            {{ dbt_utils.generate_surrogate_key(['sales_order_id']) }} as sales_order_uid
            , {{ dbt_utils.generate_surrogate_key(['territory_id']) }} as territory_uid
            , {{ dbt_utils.generate_surrogate_key(['bill_to_address_id']) }} as bill_to_address_uid
            , {{ dbt_utils.generate_surrogate_key(['ship_to_address_id']) }} as ship_to_address_uid
            , {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_uid
            , {{ dbt_utils.generate_surrogate_key(['credit_card_id']) }} as credit_card_uid
            , sales_order_id
            , due_date
            , order_date
            , ship_date
            , online_order_flag
            , taxa_mt
            , subtotal
            , total_due
            , case status
                when 1 then 'in process'
                when 2 then 'approved'
                when 3 then 'backordered'
                when 4 then 'rejected'
                when 5 then 'shipped'
                when 6 then 'cancelled'
              end as status_name
        from sales_order_header
    )

select *
from final
