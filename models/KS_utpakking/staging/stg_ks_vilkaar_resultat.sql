with ks_meta_data as (
  select * from {{ref ('stg_ks_meta_data')}}
),

pre_final as (
  select * from
  (
    select *  from ks_meta_data,
      json_table(melding, '$'
        columns(
          behandlings_id  NUMBER(38,0)  PATH  '$.behandlingsId',
          nested                        PATH '$.vilkårResultater[*]'
          columns(
            resultat      VARCHAR2(255) PATH  '$.resultat',
            antall_timer  NUMBER(10,2)  PATH  '$.antallTimer',
            periode_fom   DATE          PATH  '$.periodeFom',
            periode_tom   DATE          PATH  '$.periodeTom',
            ident         VARCHAR2(255) PATH  '$.ident',
            vilkaar_type  VARCHAR2(255) PATH  '$.vilkårType'
            )
          )
        ) j
  )
  where vilkaar_type is not null
),

final as (
    select 
        behandlings_id,
        resultat,
        antall_timer,
        periode_fom,
        periode_tom,
        ident,
        vilkaar_type,
        kafka_offset,
        kafka_mottatt_dato
    from pre_final
)

select * from final 
