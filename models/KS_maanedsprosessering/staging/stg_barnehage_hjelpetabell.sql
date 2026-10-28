with stg_barnehage_hjelpetabell as (

    select
        prosent
        ,tom_timer
        ,fom_timer
        ,belop_per_mnd

    from {{ source('fam_ks', 'barnehage_hjelpetabell') }}

)

select *
from stg_barnehage_hjelpetabell