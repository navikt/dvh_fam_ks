with dim_tid_mnd as (

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

        ,DIM_TID_MND.AAR_MAANED
        ,DIM_TID_MND.SISTE_DATO_I_PERIODEN

    from {{ ref('int_ks_join_fagsak') }} SISTE

    inner join {{ ref('stg_dim_tid_mnd') }} DIM_TID_MND
        on DIM_TID_MND.AAR_MAANED >= to_char(SISTE.STONAD_FOM, 'YYYYMM')
       and DIM_TID_MND.AAR_MAANED <= to_char(SISTE.STONAD_TOM, 'YYYYMM')
       and DIM_TID_MND.DIM_NIVAA = 3 --
       and DIM_TID_MND.GYLDIG_FLAGG = 1 --

)

select *
from dim_tid

/*
JOIN  DT_KODEVERK.DIM_TID DIM_TID_MND ON
    DIM_TID_MND.AAR_MAANED >= to_char(B.STONAD_FOM,'YYYYMM') AND
    DIM_TID_MND.AAR_MAANED <= to_char(B.STONAD_TOM,'YYYYMM') AND
    DIM_TID_MND.DIM_NIVAA=3 AND
    DIM_TID_MND.GYLDIG_FLAGG=1 
*/