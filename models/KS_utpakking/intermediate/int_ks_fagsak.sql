with ks_fagsak as (
    select * from {{ ref ('stg_ks_fagsak') }}
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
    kafka_mottatt_dato,
    fk_ks_meta_data
  from
    ks_fagsak f
  left outer join {{ source('dt_person', 'ident_off_id_til_fk_person1') }} b on
    f.person_ident = b.off_id
    and b.gyldig_fra_dato <= f.kafka_mottatt_dato
    and b.gyldig_til_dato >= f.kafka_mottatt_dato
    and b.skjermet_kode = 0
)

select *
from final

