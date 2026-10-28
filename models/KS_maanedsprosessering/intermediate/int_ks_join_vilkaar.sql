with vilkaar as (

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

        ,SISTE.BARN_FODT_DATO

        ,SISTE.KOMPETANSE_RESULTAT
        ,SISTE.KOMPETANSE_AKTIVITET

        ,VILKAAR.ANTALL_TIMER as ANTALL_TIMER
        ,HJELPETABELL.PROSENT as BARNEHAGEPLASS_PROSENT

    from {{ ref('int_ks_join_kompetanse') }} SISTE

    left join {{ ref('stg_fam_ks_vilkaar_resultat') }} VILKAAR
        on SISTE.PK_KS_FAGSAK = VILKAAR.FK_KS_FAGSAK
        and SISTE.FK_PERSON1_BARN = VILKAAR.FK_PERSON1
        and TO_DATE(
            SISTE.AAR_MAANED || '01'
            ,'YYYYMMDD'
        ) 
        between VILKAAR.PERIODE_FOM
            and NVL(
                VILKAAR.PERIODE_TOM
                ,SISTE.SISTE_DATO_I_PERIODEN
            )
        and VILKAAR.VILKAAR_TYPE = 'BARNEHAGEPLASS'

    -- Har en ekstra join for hjelpetabell istedenfor en ny int
    left join {{ ref('stg_barnehage_hjelpetabell') }} HJELPETABELL
        on VILKAAR.ANTALL_TIMER >= HJELPETABELL.FOM_TIMER
        and (
            VILKAAR.ANTALL_TIMER <= HJELPETABELL.TOM_TIMER
            or HJELPETABELL.TOM_TIMER is null
        )

    group by
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
        ,SISTE.BARN_FODT_DATO
        ,SISTE.KOMPETANSE_RESULTAT
        ,SISTE.KOMPETANSE_AKTIVITET
        ,VILKAAR.ANTALL_TIMER
        ,HJELPETABELL.PROSENT
)

select *
from vilkaar

/*
    left join dvh_fam_ks.fam_ks_vilkaar_resultat vilkaar
        on FAGSAK.pk_ks_fagsak = vilkaar.fk_ks_fagsak
        and A.FK_PERSON1_BARN = vilkaar.fk_person1
        and to_date(to_char(DIM_TID_MND.SISTE_DATO_I_PERIODEN,'yyyymm')||'01', 'yyyymmdd') between vilkaar.periode_fom and nvl(vilkaar.periode_tom, DIM_TID_MND.SISTE_DATO_I_PERIODEN)
        and vilkaar.vilkaar_type = 'BARNEHAGEPLASS'
*/