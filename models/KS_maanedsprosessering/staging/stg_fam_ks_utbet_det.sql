with stg_fam_ks_utbet_det as (

    select
        fk_ks_utbetaling
        ,fk_person1_barn
        ,klassekode
        ,utbetalt_per_mnd

    from {{ source('fam_ks', 'fam_ks_utbet_det') }}

)

select *
from stg_fam_ks_utbet_det