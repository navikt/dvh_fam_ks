with fam_ks_kompetanse_perioder as (

    select
        fk_ks_fagsak
        ,fom
        ,tom
        ,kompetanse_resultat
        ,kompetanse_aktivitet

    from {{ source('fam_ks', 'fam_ks_kompetanse_perioder') }}

)

select *
from fam_ks_kompetanse_perioder