with barn as (

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

        ,SISTE.MOTTAKER_FODT_DATO

        ,BARN.FODT_DATO BARN_FODT_DATO -- Husk alias videre

    from {{ ref('int_ks_join_mottaker') }} SISTE

    left join {{ ref('stg_dim_person') }} BARN
        on SISTE.FK_PERSON1_BARN = BARN.FK_PERSON1
       and BARN.GYLDIG_FRA_DATO <= SISTE.SISTE_DATO_I_PERIODEN
       and BARN.GYLDIG_TIL_DATO >= SISTE.SISTE_DATO_I_PERIODEN

)

select *
from barn