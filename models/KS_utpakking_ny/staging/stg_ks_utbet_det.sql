with ks_meta_data as (
  select * from {{ref ('stg_ks_meta_data')}}
),

pre_final as (
  select * from
  (
    select *  from ks_meta_data,
      json_table(melding, '$'
        columns(
            behandlings_id              NUMBER(38,0)    PATH  '$.behandlingsId',
            nested                                      PATH '$.utbetalingsperioder[*]'
            columns(
                stonad_fom              DATE            PATH '$.stønadFom',
                stonad_tom              DATE            PATH '$.stønadTom',
                nested                                  PATH '$.utbetalingsDetaljer[*]'
                columns(
                    klassekode          VARCHAR2(255)   PATH  '$.klassekode',
                    utbetalt_per_mnd    NUMBER(16,2)    PATH '$.utbetaltPrMnd',
                    delytelse_id        NUMBER(38,0)    PATH '$.delytelseId',
                    nested                              PATH '$.person'
                    columns(
                        person_ident    VARCHAR2(255)   PATH  '$.personIdent'
                        )
                    )
                )
            )
      ) j
  )
  where delytelse_id is not null
),

final as (
    select 
        behandlings_id,
        stonad_fom,
        stonad_tom,
        klassekode,
        utbetalt_per_mnd,
        delytelse_id,
        person_ident,
        kafka_offset,
        kafka_mottatt_dato
    from pre_final
)

select * 
from final