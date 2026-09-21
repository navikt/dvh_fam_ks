-- én rad per måned?
{{
  config(
    materialized='incremental',
    unique_key = ['AAR_MAANED', 'FAGSAK_ID', 'FK_PERSON1_BARN'],
    incremental_strategy='delete+insert'
  )
}}

with fact as (

    select
        SISTE.AAR_MAANED
        ,SISTE.FAGSAK_ID
        ,SISTE.FK_PERSON1_MOTTAKER
        ,to_char(SISTE.MOTTAKER_FODT_DATO, 'YYYY') as FODSELS_AAR_MOTTAKER
        ,to_char(SISTE.MOTTAKER_FODT_DATO, 'MM') as FODSELS_MND_MOTTAKER
        ,SISTE.BEHANDLING_AARSAK
        ,SISTE.FK_PERSON1_BARN
        ,to_char(SISTE.BARN_FODT_DATO, 'YYYY') as FODSELS_AAR_BARN
        ,to_char(SISTE.BARN_FODT_DATO, 'MM') as FODSELS_MND_BARN
        ,SISTE.KLASSEKODE
        ,SISTE.STONAD_FOM
        ,SISTE.STONAD_TOM
        ,SISTE.UTBETALT_PER_MND
        ,SISTE.KOMPETANSE_RESULTAT
        ,SISTE.KOMPETANSE_AKTIVITET

    from {{ ref('int_ks_join_kompetanse') }} SISTE

)

select
    AAR_MAANED
    ,FAGSAK_ID
    ,FK_PERSON1_MOTTAKER
    ,FODSELS_AAR_MOTTAKER
    ,FODSELS_MND_MOTTAKER
    ,BEHANDLING_AARSAK
    ,FK_PERSON1_BARN
    ,FODSELS_AAR_BARN
    ,FODSELS_MND_BARN
    ,KLASSEKODE
    ,STONAD_FOM
    ,STONAD_TOM
    ,UTBETALT_PER_MND
    ,KOMPETANSE_RESULTAT
    ,KOMPETANSE_AKTIVITET

from fact
-- Kunne like godt skrevet select *