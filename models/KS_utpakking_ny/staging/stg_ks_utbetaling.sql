with ks_meta_data as (
  select * from {{ref ('stg_ks_meta_data')}}
),

pre_final as (
  select *
  from
  (select *  from ks_meta_data,
    json_table(melding, '$'
      columns(
          behandlings_id            NUMBER(38,0)                PATH  '$.behandlingsId',
            nested                                  PATH '$.utbetalingsperioder[*]'
            columns(
                hjemmel             VARCHAR2(255)   PATH '$.hjemmel',
                utbetalt_per_mnd    NUMBER(16,2)    PATH '$.utbetaltPerMnd',
                stonad_fom          DATE            PATH '$.stønadFom',
                stonad_tom          DATE            PATH  '$.stønadTom'
          )
        )
      ) j
    )
    where stonad_fom is not null
),

final as (
    select 
        behandlings_id,
        hjemmel,
        utbetalt_per_mnd,
        stonad_fom,
        stonad_tom,
        kafka_offset
    from pre_final
)

select * from final