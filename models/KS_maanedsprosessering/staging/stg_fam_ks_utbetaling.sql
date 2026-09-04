with fam_ks_utbetaling as (

    select
        pk_ks_utbetaling
        ,fk_ks_fagsak
        ,stonad_fom
        ,stonad_tom

    from {{ source('fam_ks', 'fam_ks_utbetaling') }}

)

select *
from fam_ks_utbetaling