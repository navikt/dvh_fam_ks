with stg_fam_ks_vilkaar_resultat as (

    select
        PK_KS_VILKAAR_RESULTAT
        ,RESULTAT
        ,ANTALL_TIMER
        ,PERIODE_FOM
        ,PERIODE_TOM
        ,FK_PERSON1
        ,VILKAAR_TYPE
        ,FK_KS_FAGSAK
        ,KAFKA_OFFSET
        ,LASTET_DATO

    from {{ source('fam_ks', 'fam_ks_vilkaar_resultat') }}

)

select *
from stg_fam_ks_vilkaar_resultat