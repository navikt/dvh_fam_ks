with mottaker as (

    select
        SISTE.FK_KS_UTBETALING
        ,SISTE.FK_PERSON1_BARN
        ,SISTE.KLASSEKODE
        ,SISTE.UTBETALT_PER_MND

        ,SISTE.FK_KS_FAGSAK
        ,SISTE.STONAD_FOM
        ,SISTE.STONAD_TOM

        ,SISTE.PK_KS_FAGSAK
        ,SISTE.FAGSAK_ID
        ,SISTE.FK_PERSON1_MOTTAKER
        ,SISTE.BEHANDLING_AARSAK

        ,SISTE.AAR_MAANED
        ,SISTE.SISTE_DATO_I_PERIODEN

        ,MOTTAKER.FODT_DATO MOTTAKER_FODT_DATO -- Husk alias videre

    from {{ ref('int_ks_join_dim_tid_mnd') }} SISTE

    left join {{ ref('stg_dim_person') }} MOTTAKER
        on SISTE.FK_PERSON1_MOTTAKER = MOTTAKER.FK_PERSON1
       and MOTTAKER.GYLDIG_FRA_DATO <= SISTE.SISTE_DATO_I_PERIODEN
       and MOTTAKER.GYLDIG_TIL_DATO >= SISTE.SISTE_DATO_I_PERIODEN

)

select *
from mottaker