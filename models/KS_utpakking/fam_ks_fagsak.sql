{{
    config(
        materialized='incremental'
    )
}}

with ks_meta_data as (
  select * from {{ref ('ks_meldinger_til_aa_pakke_ut')}}
),

pre_final as (
select * from ks_meta_data,
  json_table(melding, '$'
    columns(
      fagsak_id  NUMBER(38,0) PATH  '$.fagsakId',
      behandlings_id  NUMBER(38,0) PATH '$.behandlingsId',
      tidspunkt_vedtak  TIMESTAMP(9) PATH '$.tidspunktVedtak',
      kategori  VARCHAR2(255) PATH '$.kategori',
      behandling_type  VARCHAR2(255) PATH '$.behandlingType',
      funksjonell_id  VARCHAR2(255) PATH '$.funksjonellId',
      behandling_aarsak  VARCHAR2(255) PATH '$.behandlingÅrsak',
        nested PATH '$.person'
          columns(
            person_ident VARCHAR2(255) PATH '$.personIdent',
            rolle VARCHAR2(255) PATH '$.rolle',
            bosteds_land VARCHAR2(255) PATH '$.bostedsland',
            delingsprosent_ytelse NUMBER(38,0) PATH '$.delingsprosentYtelse'
        )
    )
    ) j
),

final as (
  select
    behandlings_id as pk_ks_fagsak,
    kafka_offset,
    fagsak_id,
    behandlings_id,
    tidspunkt_vedtak,
    kategori,
    behandling_type,
    funksjonell_id,
    behandling_aarsak,
    person_ident,
    nvl(b.fk_person1, -1) fk_person1_mottaker,
    rolle,
    bosteds_land,
    delingsprosent_ytelse,
    sysdate lastet_dato,
    kafka_mottatt_dato,
    pk_ks_meta_data as fk_ks_meta_data
  from
    pre_final
  left outer join {{ source('dt_person', 'ident_off_id_til_fk_person1') }} b on
    pre_final.person_ident=b.off_id
    and b.gyldig_fra_dato<=pre_final.kafka_mottatt_dato
    and b.gyldig_til_dato>=kafka_mottatt_dato
    and b.skjermet_kode=0
)

select
  pk_ks_fagsak,
  kafka_offset,
  fagsak_id,
  behandlings_id,
  tidspunkt_vedtak,
  kategori,
  behandling_type,
  funksjonell_id,
  behandling_aarsak,
  fk_person1_mottaker,
  rolle,
  bosteds_land,
  delingsprosent_ytelse,
  lastet_dato,
  kafka_mottatt_dato,
  fk_ks_meta_data
from final