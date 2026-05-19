{{
    config(
        materialized='incremental'
    )
}}


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
    localtimestamp lastet_dato,
    kafka_mottatt_dato,
    fk_ks_meta_data
from {{ ref ('int_ks_fagsak') }}

{% if is_incremental() %}
    WHERE kafka_offset > COALESCE(( SELECT MAX(t.kafka_offset) FROM {{ this }} t ), 0)
{% endif %}