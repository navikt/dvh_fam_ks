{{
    config(
        materialized='incremental'
    )
}}

select 
    PK_KS_KOMPETANSE_BARN
    ,FK_KS_KOMPETANSE_PERIODER
    ,FK_PERSON1
    ,kafka_offset
    ,localtimestamp as lastet_dato

from {{ref ('int_ks_kompetanse_barn')}}

{% if is_incremental() %}
    WHERE kafka_offset > COALESCE(( SELECT MAX(t.kafka_offset) FROM {{ this }} t ), 0)
{% endif %}