with
    credit_card as (
        select * from {{ ref('stg_erp__credit_card') }}
    ),
    final as (
        select
            credit_card.credit_card_uid
            , credit_card.credit_card_id
            , credit_card.card_type
        from credit_card
    )
select *
from final
