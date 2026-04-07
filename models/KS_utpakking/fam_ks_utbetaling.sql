{{
    config(
        materialized='incremental'
    )
}}

with ks_meta_data as (
  select * from {{ref ('ks_meldinger_til_aa_pakke_ut')}}
),

ks_fagsak as (
    select behandlings_id, pk_ks_fagsak, kafka_offset
    from {{ref ('fam_ks_fagsak')}}
),

pre_final as (
  select *
  from
  (select *  from ks_meta_data,
    json_table(melding, '$'
      columns(
          behandlings_id   PATH  '$.behandlingsId',
            nested path '$.utbetalingsperioder[*]'
            columns(
              hjemmel VARCHAR2(255) PATH '$.hjemmel',
              utbetalt_per_mnd NUMBER(16,2) PATH '$.utbetaltPerMnd',
              stonad_fom    PATH '$.stønadFom',
              stonad_tom    PATH  '$.stønadTom'
          )
        )
      ) j
    )
    where stonad_fom is not null
),

final as (
select
  to_number(replace(pre_final.behandlings_id || stonad_fom || stonad_tom, '-', '')) as pk_ks_utbetaling,
  pre_final.kafka_offset,
  hjemmel,
  utbetalt_per_mnd,
  to_date(stonad_fom, 'yyyy-mm-dd') stonad_fom,
  to_date(stonad_tom,'yyyy-mm-dd') stonad_tom,
  sysdate lastet_dato,
  ks_fagsak.pk_ks_fagsak as fk_ks_fagsak
from pre_final
join ks_fagsak
  on pre_final.kafka_offset = ks_fagsak.kafka_offset
  and pre_final.behandlings_id = ks_fagsak.behandlings_id
)

select * from final