with  
    customer as (
        select * from {{ ref("stg_erp__customer") }}
    ),
    person as (
        select * from {{ ref("stg_erp__person") }}
    ),
    store as (
        select * from {{ ref('stg_erp__store') }}
    ),
    final AS (
        select
            customer.customer_uid
            , customer.customer_id
            , customer.customer_type
            , COALESCE(person.person_name, store.store_name) as customer_name
        from customer
        left join person on customer.person_uid = person.person_uid
        left join store on customer.store_uid = store.store_uid
    )

select *
from final