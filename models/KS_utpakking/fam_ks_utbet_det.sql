{{
    config(
        materialized='incremental'
    )
}}

with ks_meta_data as (
  select * from {{ref ('ks_meldinger_til_aa_pakke_ut')}}
),

ks_utbetaling as (
  select * from {{ref('fam_ks_utbetaling')}}
),

pre_final as (
  select * from
  (
    select *  from ks_meta_data,
      json_table(melding, '$'
        columns(
          behandlings_id  PATH  '$.behandlingsId',
            nested path '$.utbetalingsperioder[*]'
            columns(
              stonad_fom     PATH '$.stønadFom',
              stonad_tom     PATH '$.stønadTom',
              nested path '$.utbetalingsDetaljer[*]'
              columns(
                klassekode VARCHAR2(255) PATH  '$.klassekode',
                utbetalt_per_mnd NUMBER(16,2) PATH '$.utbetaltPrMnd',
                delytelse_id     PATH '$.delytelseId',
                nested path '$.person'
                  columns(
                    person_ident VARCHAR2(255) PATH  '$.personIdent'
                    )
                ))
          )
      ) j
  )
  where delytelse_id is not null
),

final as (
select
to_number(replace(behandlings_id || stonad_fom || stonad_tom || delytelse_ID, '-', '')) as pk_ks_utbet_det,
kafka_offset,
klassekode,
utbetalt_per_mnd,
delytelse_id,
person_ident,
nvl(b.fk_person1, -1) fk_person1_barn,
to_date(stonad_fom, 'yyyy-mm-dd') stonad_fom,
to_date(stonad_tom,'yyyy-mm-dd') stonad_tom,
kafka_mottatt_dato,
sysdate lastet_dato
from
  pre_final 
left outer join dt_person.ident_off_id_til_fk_person1 b on
  person_ident=b.off_id
  and b.gyldig_fra_dato <= kafka_mottatt_dato
  and b.gyldig_til_dato >= kafka_mottatt_dato
  and b.skjermet_kode = 0
)

select
  pk_ks_utbet_det,
  f.kafka_offset,
  utbetalt_per_mnd,
  lastet_dato,
  delytelse_id,
  fk_person1_barn,
  u.pk_ks_utbetaling as fk_ks_utbetaling,
  klassekode
from final f
join ks_utbetaling u
    on f.stonad_fom = u.stonad_fom 
    and f.stonad_tom = u.stonad_tom 
    and f.kafka_offset = u.kafka_offset