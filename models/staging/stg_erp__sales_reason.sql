with
    sales_reason as (
        select
            cast(salesreasonid as int) as sales_reason_id
            , cast(reasontype as varchar) as reason_type
            , cast(name as varchar) as reason_name
        from {{ source('erp', 'salesreason') }}  
    )

    , final as (
        select
            {{ dbt_utils.generate_surrogate_key(['sales_reason_id']) }} as sales_reason_uid
            , sales_reason_id
            , reason_type
            , reason_name
        from sales_reason
    )

select *
from final
