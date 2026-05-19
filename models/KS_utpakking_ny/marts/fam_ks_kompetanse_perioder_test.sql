{{
    config(
        materialized='incremental'
    )
}}

select
    PK_KS_KOMPETANSE_PERIODER
    ,FOM
    ,TOM
    ,FK_KS_FAGSAK
    ,KOMPETANSE_AKTIVITET
    ,SOKERS_AKTIVITETSLAND
    ,ANNEN_FORELDERS_AKTIVITET
    ,ANNEN_FORELDERS_AKTIVITETSLAND
    ,BARNETS_BOSTEDSLAND
    ,kompetanse_Resultat
    ,ANNEN_FORELDER_OMFATTET_AV_NORSK_LOVGIVNING
    ,localtimestamp lastet_dato
    ,kafka_offset
from {{ref ('int_ks_kompetanse_perioder')}}

{% if is_incremental() %}
    WHERE kafka_offset > COALESCE(( SELECT MAX(t.kafka_offset) FROM {{ this }} t ), 0)
{% endif %}