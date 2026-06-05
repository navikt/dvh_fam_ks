{{
    config(
        materialized='incremental'
    )
}}

select
    pk_ks_vilkaar_resultat
    ,resultat
    ,antall_timer
    ,periode_fom
    ,periode_tom
    /*
    ,case when fk_person1 = -1 then ident
        else cast(null as varchar2(11))
    end ident
    */
    ,fk_person1
    ,vilkaar_type
    ,fk_ks_fagsak
    ,kafka_offset
    ,localtimestamp as lastet_dato
from {{ref ('int_ks_vilkaar_resultat')}}

{% if is_incremental() %}
    WHERE kafka_offset > COALESCE(( SELECT MAX(t.kafka_offset) FROM {{ this }} t ), 0)
{% endif %}