with fagsak_siste as (

    select
        SISTE.FK_KS_UTBETALING
        ,SISTE.FK_PERSON1_BARN
        ,SISTE.KLASSEKODE
        ,SISTE.UTBETALT_PER_MND
        
        ,SISTE.FK_KS_FAGSAK
        ,SISTE.STONAD_FOM
        ,SISTE.STONAD_TOM

        ,FAGSAK_SISTE.PK_KS_FAGSAK -- Denne modellen tilfører ingen nye kolonner, men speiler originalviewets join-rekkefølge. Dette er for økt lesbarhet.

    from {{ ref('int_ks_join_utbetaling') }} SISTE

    left join {{ ref('stg_vfam_ks_fagsak_siste') }} FAGSAK_SISTE
        on SISTE.FK_KS_FAGSAK = FAGSAK_SISTE.PK_KS_FAGSAK

)

select *
from fagsak_siste