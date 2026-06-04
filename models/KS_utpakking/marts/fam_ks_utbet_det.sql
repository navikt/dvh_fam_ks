{{
    config(
        materialized='incremental'
    )
}}

select
    pk_ks_utbet_det
    ,pk_ks_utbet_det_ny
    ,kafka_offset
    ,utbetalt_per_mnd
    ,delytelse_id
    ,fk_person1_barn
    ,fk_ks_utbetaling
    ,klassekode
    ,localtimestamp as lastet_dato
from {{ref ('int_ks_utbet_det')}}

{% if is_incremental() %}
    WHERE kafka_offset > COALESCE(( SELECT MAX(t.kafka_offset) FROM {{ this }} t ), 0)
{% endif %}
