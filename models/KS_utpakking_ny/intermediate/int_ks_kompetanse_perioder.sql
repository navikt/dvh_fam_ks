with ks_fagsak as (
    select behandlings_id, pk_ks_fagsak, kafka_offset
    from {{ref ('int_ks_fagsak')}}
),

kompetanse_perioder as (
    select *
    from {{ref ('stg_ks_kompetanse_perioder')}}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['k.behandlings_id ', 'k.kafka_offset ', 'k.fom', 'k.tom']) }} PK_KS_KOMPETANSE_PERIODER,
        --STANDARD_HASH(k.behandlings_id || k.kafka_offset || NVL(k.fom, '-') || NVL(k.tom, '-'), 'MD5') PK_KS_KOMPETANSE_PERIODER,
        f.pk_ks_fagsak as fk_ks_fagsak,
        TO_CHAR(TO_DATE(fom, 'YYYY-MM'), 'YYYYMM')  fom,
        TO_CHAR(TO_DATE(tom, 'YYYY-MM'), 'YYYYMM')  tom,
        kompetanse_aktivitet,
        kompetanse_Resultat,
        barnets_bostedsland,
        k.kafka_offset,
        SOKERS_AKTIVITETSLAND,
        ANNEN_FORELDERS_AKTIVITET,
        ANNEN_FORELDERS_AKTIVITETSLAND,
        CASE 
            WHEN ANNEN_FORELDER_OMFATTET_AV_NORSK_LOVGIVNING = 'false' then 0
            ELSE 1
        END ANNEN_FORELDER_OMFATTET_AV_NORSK_LOVGIVNING
    from kompetanse_perioder k
    join ks_fagsak  f
    on k.kafka_offset = f.kafka_offset
    and k.behandlings_id = f.behandlings_id
)

select 
    PK_KS_KOMPETANSE_PERIODER,
    fk_ks_fagsak,
    fom,
    tom,
    kompetanse_aktivitet,
    kompetanse_Resultat,
    barnets_bostedsland,
    kafka_offset,
    SOKERS_AKTIVITETSLAND,
    ANNEN_FORELDERS_AKTIVITET,
    ANNEN_FORELDERS_AKTIVITETSLAND,  
    ANNEN_FORELDER_OMFATTET_AV_NORSK_LOVGIVNING  
from final