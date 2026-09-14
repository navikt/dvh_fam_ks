with utbetaling as (

    -- For å øke lesbarheten, kommer jeg i kommende modeller til å skrive SISTE.[kolonne] over SISTE.*. Dette gir et tydligere bilde på hva som brukes.
    -- Denne kommentaren ligger her, siden dette er første int sett i lineage.

    select
        UTBET_DET.FK_KS_UTBETALING
        ,UTBET_DET.FK_PERSON1_BARN
        ,UTBET_DET.KLASSEKODE
        ,UTBET_DET.UTBETALT_PER_MND

        ,UTBETALING.FK_KS_FAGSAK
        ,UTBETALING.STONAD_FOM
        ,UTBETALING.STONAD_TOM

    from {{ ref('stg_fam_ks_utbet_det') }} UTBET_DET

    inner join {{ ref('stg_fam_ks_utbetaling') }} UTBETALING
        on UTBET_DET.FK_KS_UTBETALING = UTBETALING.PK_KS_UTBETALING

)

select *
from utbetaling