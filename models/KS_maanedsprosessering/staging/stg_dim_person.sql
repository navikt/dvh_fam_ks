with dim_person as (

    select
        fk_person1
        ,fodt_dato
        ,gyldig_fra_dato
        ,gyldig_til_dato

    from {{ source('dt_person', 'dim_person') }}

)

select *
from dim_person

--brukes flere ganger, løses med aliasering