with
    credit_card as (  
        select 
            cast(cardtype as varchar) as card_type
            , cast(creditcardid as int) as credit_card_id
        from {{ source('erp', 'creditcard') }}  
    )

    , final as (
        select 
            {{ dbt_utils.generate_surrogate_key(['credit_card_id']) }} as credit_card_uid
            , credit_card_id
            , card_type
        from credit_card
    )

select *
from final
