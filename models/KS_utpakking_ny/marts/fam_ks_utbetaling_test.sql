{{
    config(
        materialized='incremental'
    )
}}

select 
    pk_ks_utbetaling
    ,pk_ks_utbetaling_ny
    ,hjemmel
    ,utbetalt_per_mnd
    ,stonad_fom
    ,stonad_tom
    ,localtimestamp lastet_dato
    ,fk_ks_fagsak
    ,kafka_offset
from {{ref ('int_ks_utbetaling')}}

{% if is_incremental() %}
    WHERE kafka_offset > COALESCE(( SELECT MAX(t.kafka_offset) FROM {{ this }} t ), 0)
{% endif %}