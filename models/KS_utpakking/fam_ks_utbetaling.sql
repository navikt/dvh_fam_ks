{{
    config(
        materialized='incremental'
    )
}}

with ks_meta_data as (
  select * from {{ref ('ks_meldinger_til_aa_pakke_ut')}}
),

ks_fagsak as (
  select * from {{ref('fam_ks_fagsak')}}
),

pre_final as (
  select *
  from
  (select *  from ks_meta_data,
    json_table(melding, '$'
      columns(
          behandlings_id  NUMBER(38,0) PATH  '$.behandlingsId',
            nested path '$.utbetalingsperioder[*]'
            columns(
              hjemmel VARCHAR2(255) PATH '$.hjemmel',
              utbetalt_per_mnd NUMBER(16,2) PATH '$.utbetaltPerMnd',
              stonad_fom   DATE PATH '$.stønadFom',
              stonad_tom   DATE PATH  '$.stønadTom'
          )
        )
      ) j
    )
    where stonad_fom is not null
),

final as (
select
  to_number(replace(behandlings_id || stonad_fom || stonad_tom, '-', '')) as pk_ks_utbetaling,
  kafka_offset,
  hjemmel,
  utbetalt_per_mnd,
  stonad_fom,
  stonad_tom,
  sysdate lastet_dato,
  behandlings_id as fk_ks_fagsak
from pre_final
)

select * from final