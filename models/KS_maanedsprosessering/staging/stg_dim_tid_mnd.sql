with stg_dim_tid_mnd as (

    select
        dim_nivaa
        ,aar_maaned
        ,gyldig_flagg
        ,siste_dato_i_perioden

    from {{ source('kode_verk', 'dim_tid') }}

)

select *
from stg_dim_tid_mnd