with
    person as (
        select
            cast(businessentityid as int) as person_id
            , cast(persontype as varchar) as person_type
            , cast(coalesce(firstname, '') as varchar) as first_name
            , cast(coalesce(middlename, '') as varchar) as middle_name
            , cast(coalesce(lastname, '') as varchar) as last_name
        from {{ source('erp', 'person') }}
    ),

    final as (
    
        select
            {{ dbt_utils.generate_surrogate_key(['person_id']) }} as person_uid
            , person_id
            , person_type
            , trim(concat(first_name, ' ', middle_name, ' ', last_name)) as person_name
        from person
    )

select *
from final
