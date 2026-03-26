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
          behandlings_id  NUMBER(38,0) PATH  '$.behandlingsId',
            nested path '$.utbetalingsperioder[*]'
            columns(
              stonad_fom    DATE  PATH '$.stønadFom',
              stonad_tom    DATE  PATH '$.stønadTom',
              nested path '$.utbetalingsDetaljer[*]'
              columns(
                klassekode VARCHAR2(255) PATH  '$.klassekode',
                utbetalt_per_mnd NUMBER(16,2) PATH '$.utbetaltPrMnd',
                delytelse_id     NUMBER(16,2) PATH '$.delytelseId',
                nested path '$.person'
                  columns(
                    person_ident VARCHAR2(255) PATH  '$.personIdent'
                    --rolle path '$.rolle',
                    --bosteds_land path '$.bostedsland',
                    --delingsprosent_ytelse path '$.delingsprosentYtelse'
                    )
                ))
          )
      ) j
  )
  where delytelse_id is not null
--where json_exists(melding, '$.utbetalingsperioder.utbetalingsDetaljer.delytelseId')
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
--rolle,
stonad_fom,
stonad_tom,
--bosteds_land,
--delingsprosent_ytelse,
kafka_mottatt_dato,
sysdate lastet_dato,
to_number(replace(behandlings_id || stonad_fom || stonad_tom, '-', '')) as fk_ks_utbetaling
from
  pre_final
left outer join {{ source('dt_person', 'ident_off_id_til_fk_person1') }} b on
  pre_final.person_ident=b.off_id
  and b.gyldig_fra_dato<=pre_final.kafka_mottatt_dato
  and b.gyldig_til_dato>=kafka_mottatt_dato
  and b.skjermet_kode=0
)

select
  pk_ks_utbet_det,
  kafka_offset,
  --hjemmel,
  utbetalt_per_mnd,
  --kafka_mottatt_dato,
  lastet_dato,
  delytelse_id,
  fk_person1_barn,
  fk_ks_utbetaling,
  klassekode
from final