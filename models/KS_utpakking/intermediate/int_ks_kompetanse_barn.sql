with kompetanse_barn as (
    select *
    from {{ref ('stg_ks_kompetanse_barn')}}
),

kompetanse_perioder as (
    select *
    from {{ref ('int_ks_kompetanse_perioder')}}
),

pre_final as (
    select
        personidentbarn,
        nvl(b.fk_person1, -1) fk_person1,
        TO_CHAR(TO_DATE(fom, 'YYYY-MM'), 'YYYYMM')  fom,
        TO_CHAR(TO_DATE(tom, 'YYYY-MM'), 'YYYYMM')  tom,
        kompetanse_Resultat,
        sokers_aktivitetsland,
        annen_forelders_aktivitet,
        annen_forelders_aktivitetsland,
        kafka_offset,
        kafka_mottatt_dato,
        barnets_bostedsland
    from
        kompetanse_barn k
    left outer join {{ source('dt_person', 'ident_off_id_til_fk_person1') }} b on
        k.personidentbarn = b.off_id
        and b.gyldig_fra_dato <= k.kafka_mottatt_dato
        and b.gyldig_til_dato >= kafka_mottatt_dato
        and b.skjermet_kode = 0 
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['k.pK_ks_KOMPETANSE_PERIODER', 'p.fk_person1']) }}  PK_KS_KOMPETANSE_BARN,
        --STANDARD_HASH(k.pK_ks_KOMPETANSE_PERIODER || p.fk_person1, 'MD5') as PK_KS_KOMPETANSE_BARN,
        p.fk_person1,
        p.fom,
        p.tom,
        p.kafka_offset,
        p.kompetanse_Resultat,
        p.kafka_mottatt_dato,
        p.barnets_bostedsland,
        k.pK_ks_KOMPETANSE_PERIODER as fK_ks_KOMPETANSE_PERIODER
  from pre_final p
  join kompetanse_perioder k
  on  COALESCE(p.fom,'-1') = COALESCE(k.fom,'-1') 
  and COALESCE(p.tom,'-1') = COALESCE(k.tom,'-1')
  and COALESCE(p.kompetanse_Resultat,'-1') = COALESCE(k.kompetanse_Resultat,'-1')
  and COALESCE(p.barnets_bostedsland,'-1') = COALESCE(k.barnets_bostedsland,'-1')
  and COALESCE(p.sokers_aktivitetsland,'-1') = COALESCE(k.sokers_aktivitetsland,'-1')
  and COALESCE(p.annen_forelders_aktivitet,'-1') = COALESCE(k.annen_forelders_aktivitet,'-1')
  and COALESCE(p.annen_forelders_aktivitetsland,'-1') = COALESCE(k.annen_forelders_aktivitetsland,'-1')
  and p.kafka_offset = k.kafka_offset
)

select * 
from final