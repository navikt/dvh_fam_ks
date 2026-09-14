with fagsak as (

    select
        SISTE.FK_KS_UTBETALING
        ,SISTE.FK_PERSON1_BARN
        ,SISTE.KLASSEKODE
        ,SISTE.UTBETALT_PER_MND

        ,SISTE.FK_KS_FAGSAK
        ,SISTE.STONAD_FOM
        ,SISTE.STONAD_TOM

        ,FAGSAK.PK_KS_FAGSAK
        ,FAGSAK.FAGSAK_ID
        ,FAGSAK.FK_PERSON1_MOTTAKER
        ,FAGSAK.BEHANDLING_AARSAK

    from {{ ref('int_ks_join_fagsak_siste') }} SISTE

    inner join {{ ref('stg_fam_ks_fagsak') }} FAGSAK
        on SISTE.FK_KS_FAGSAK = FAGSAK.PK_KS_FAGSAK --

)

select *
from fagsak