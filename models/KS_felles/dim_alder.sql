{{ config(materialized='ephemeral') }}

select pk_dim_alder as fk_dim_alder
    ,alder
from {{ source ('kode_verk', 'dim_alder') }}
where gyldig_flagg = 1