with vfam_ks_fagsak_siste as (

    select *

    from {{ source('fam_ks', 'vfam_ks_fagsak_siste') }}

)

select *
from vfam_ks_fagsak_siste