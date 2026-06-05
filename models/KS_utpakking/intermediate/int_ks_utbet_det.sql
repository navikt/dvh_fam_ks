with utbet_det as (
    select *
    from {{ref ('stg_ks_utbet_det')}}
),

utbetaling as (
    select *
    from {{ref ('int_ks_utbetaling')}}
),

pre_final as (
    select
        --to_number(replace(behandlings_id || stonad_fom || stonad_tom || delytelse_ID, '-', '')) as pk_ks_utbet_det,    TO_CHAR(stonad_fom, 'YYYYMMDD')
        --to_number(replace(behandlings_id || kafka_offset || TO_CHAR(stonad_fom, 'YYYYMMDD') || TO_CHAR(stonad_tom, 'YYYYMMDD') || delytelse_ID, '-', '')) as pk_ks_utbet_det,
        --to_number(replace(behandlings_id || TO_CHAR(stonad_fom, 'YYYYMMDD') || TO_CHAR(stonad_tom, 'YYYYMMDD') || delytelse_ID, '-', '')) as pk_ks_utbet_det,
        {{ dbt_utils.generate_surrogate_key(['behandlings_id', 'kafka_offset', 'stonad_fom', 'stonad_tom', 'delytelse_ID']) }}  as pk_ks_utbet_det,
        --STANDARD_HASH(behandlings_id || kafka_offset || NVL(TO_CHAR(stonad_fom, 'YYYYMMDD'), 'x') || NVL(TO_CHAR(stonad_tom, 'YYYYMMDD'), 'x') || NVL(TO_CHAR(delytelse_ID), 'x'), 'MD5') as pk_ks_utbet_det_ny,
        kafka_offset,
        klassekode,
        utbetalt_per_mnd,
        delytelse_id,
        person_ident,
        nvl(b.fk_person1, -1) fk_person1_barn,
         stonad_fom,
         stonad_tom,
        kafka_mottatt_dato
    from utbet_det u
    left outer join dt_person.ident_off_id_til_fk_person1 b 
    on u.person_ident = b.off_id
    and b.gyldig_fra_dato <= u.kafka_mottatt_dato
    and b.gyldig_til_dato >= u.kafka_mottatt_dato
    and b.skjermet_kode = 0
)

select
    pk_ks_utbet_det,
    p.kafka_offset,
    p.utbetalt_per_mnd,
    p.delytelse_id,
    p.fk_person1_barn,
    u.pk_ks_utbetaling as fk_ks_utbetaling,
    p.klassekode
from pre_final p
join utbetaling u
on  p.stonad_fom = u.stonad_fom 
and p.stonad_tom = u.stonad_tom 
and p.kafka_offset = u.kafka_offset