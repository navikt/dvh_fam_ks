with ks_meta_data as (
  select * from {{ref ('stg_ks_meta_data')}}
),

pre_final as (
select * from ks_meta_data,
  json_table(melding, '$'
    columns(
      fagsak_id                         NUMBER(38,0)    PATH    '$.fagsakId',
      behandlings_id                    NUMBER(38,0)    PATH    '$.behandlingsId',
      tidspunkt_vedtak                  TIMESTAMP(9)    PATH    '$.tidspunktVedtak',
      kategori                          VARCHAR2(255)   PATH    '$.kategori',
      behandling_type                   VARCHAR2(255)   PATH    '$.behandlingType',
      funksjonell_id                    VARCHAR2(255)   PATH    '$.funksjonellId',
      behandling_aarsak                 VARCHAR2(255)   PATH    '$.behandlingÅrsak',
        nested PATH '$.person'
          columns(
                person_ident            VARCHAR2(255)   PATH    '$.personIdent',
                rolle                   VARCHAR2(255)   PATH    '$.rolle',
                bosteds_land            VARCHAR2(255)   PATH    '$.bostedsland',
                delingsprosent_ytelse   NUMBER(38,0)    PATH    '$.delingsprosentYtelse'
        )
    )
    ) j
),

final as (
  select
    behandlings_id,
    kafka_offset,
    fagsak_id,
    tidspunkt_vedtak,
    kategori,
    behandling_type,
    funksjonell_id,
    behandling_aarsak,
    person_ident,
    rolle,
    bosteds_land,
    delingsprosent_ytelse,
    pk_ks_meta_data as fk_ks_meta_data,
    kafka_mottatt_dato
  from pre_final
)

select * 
from final