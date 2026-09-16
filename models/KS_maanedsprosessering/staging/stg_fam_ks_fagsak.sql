with stg_fam_ks_fagsak as (

    select
        pk_ks_fagsak
        ,fagsak_id
        ,fk_person1_mottaker
        ,behandling_aarsak

    from {{ source('fam_ks', 'fam_ks_fagsak') }}

)

select *
from stg_fam_ks_fagsak