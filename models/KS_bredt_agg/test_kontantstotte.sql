{{
    config(
        materialized='table'
    )
}}

/* 
Kontantstøtte
*/

with fag as (
    select  TO_CHAR(t1.tidspunkt_vedtak, 'yyyymm') || '003' as fk_dim_tid
        ,TO_CHAR(t1.tidspunkt_vedtak, 'yyyymm') as aar_maaned
        ,t1.tidspunkt_vedtak as vedtakstidspunkt
        ,t1.fk_person1_mottaker
        ,t1.behandlings_id 
        from fam_ks_fagsak t1
),

/* 
Kobler inn felter fra dim_person_keys.
Fødselsdato (yyyymm) mot vedtakstidspunktet (yyyymm). Alder beregnes som antall måneder mellom disse, hvor det så deles på 12, og runder ned til nærmeste år.  
*/

pre_final as (
    select t1.*
    ,{{ dbt_dvh_macros.BREDAGG__ephemeral_star(model_name='dim_person_keys', relation_alias='t2', prefix='MOTTAKER_', except=["fk_person1","gyldig_fra_dato", "gyldig_til_dato" ]) }}
    ,trunc(months_between(to_date(aar_maaned, 'yyyymm'), to_date(t3.fodt_aar_maaned, 'yyyymm')) / 12) AS mottaker_alder    
    from fag t1
    left join  {{ ref('dim_person_keys') }} t2
    on t1.fk_person1_mottaker = t2.fk_person1
        and t2.gyldig_fra_dato <= t1.vedtakstidspunkt
        and t2.gyldig_til_dato >= t1.vedtakstidspunkt
-- alder
    left join  {{ ref('dim_person_fodt') }} t3
    on t1.fk_person1_mottaker = t3.fk_person1
),


/* 
Kobler inn alder-nøkkel.
*/

final as (
    select t1.*
    ,{{ dbt_dvh_macros.BREDAGG__ephemeral_star(model_name='dim_alder', relation_alias='t2', prefix='MOTTAKER_', except=["alder"]) }}
    from pre_final t1
    left join  {{ ref('dim_alder') }} t2
    on t1.mottaker_alder = t2.alder
)

select * from final