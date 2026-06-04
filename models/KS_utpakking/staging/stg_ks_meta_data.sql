
select
    pk_ks_meta_data,
    kafka_offset,
    kafka_partisjon, 
    kafka_mottatt_dato,
    kafka_topic,
    melding
from {{ source('fam_ks', 'fam_ks_meta_data') }}
