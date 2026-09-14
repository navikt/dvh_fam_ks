with vfam_ks_fagsak_siste as (

    select *

    from {{ source('fam_ks', 'vfam_ks_fagsak_siste') }}

)

select *
from vfam_ks_fagsak_siste
-- Denne modellen tilfører ingen nye kolonner, men speiler originalviewets join-rekkefølge. Dette er kun for økt lesbarhet.