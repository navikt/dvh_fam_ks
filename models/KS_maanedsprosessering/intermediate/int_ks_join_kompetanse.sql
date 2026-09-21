with kompetanse as (

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

        ,KOMP_PER.KOMPETANSE_RESULTAT
        ,KOMP_PER.KOMPETANSE_AKTIVITET

    from {{ ref('int_ks_join_barn') }} SISTE

    left join {{ ref('stg_fam_ks_kompetanse_perioder') }} KOMP_PER
        on SISTE.PK_KS_FAGSAK = KOMP_PER.FK_KS_FAGSAK --
       and SISTE.AAR_MAANED >= to_char(
            to_date(KOMP_PER.FOM, 'YYYY-MM')
           ,'YYYYMM'
       )
       and SISTE.AAR_MAANED <= to_char(
            to_date(nvl(KOMP_PER.TOM, '2999-12'), 'YYYY-MM')
           ,'YYYYMM'
       )
       and KOMP_PER.FOM is not null

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
        ,KOMP_PER.KOMPETANSE_RESULTAT
        ,KOMP_PER.KOMPETANSE_AKTIVITET
)

select *
from kompetanse

/*
left outer join FAM_KS_KOMPETANSE_PERIODER KOMP_PER ON
    KOMP_PER.FK_KS_FAGSAK=FAGSAK.PK_KS_FAGSAK AND
    DIM_TID_MND.AAR_MAANED >= to_char(to_date(KOMP_PER.FOM,'YYYY-MM'),'YYYYMM') AND
    DIM_TID_MND.AAR_MAANED <= to_char(to_date(nvl(KOMP_PER.TOM,'2999-12'),'YYYY-MM') ,'YYYYMM')
    and KOMP_PER.FOM IS NOT NULL
*/
