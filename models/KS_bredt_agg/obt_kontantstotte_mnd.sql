{{
    config(
        materialized='table',
        post_hook="{{ dbt_dvh_macros.BREDAGG__sync_multi_source_comments([ ['kode_verk', 'dim_tid'], 
		['kode_verk', 'dim_geografi'], ['kode_verk', 'dim_alder'],
        ['kode_verk', 'dim_kjonn']]) }}"
    )
}}

with mot as (
    select fk_dim_tid
    ,mottaker_fk_dim_geografi
    ,mottaker_fk_dim_alder
    ,mottaker_fk_dim_kjonn
    ,count(distinct fk_person1_mottaker) as kontantstotte_antall_mottakere
    from  {{ ref('test_kontantstotte') }}
    group by fk_dim_tid
    ,mottaker_fk_dim_geografi
    ,mottaker_fk_dim_alder
    ,mottaker_fk_dim_kjonn
),

final as ( 
    select 
    'Kontantstøtte' as kilde_omraade,
    {{ dbt_dvh_macros.BREDAGG__ephemeral_star(model_name='dim_skjelett', relation_alias='t1') }},
    t2.kontantstotte_antall_mottakere
    from {{ ref('dim_skjelett') }} t1
    left join mot t2
    on t1.fk_dim_tid = t2.fk_dim_tid
    and t1.fk_dim_geografi = t2.mottaker_fk_dim_geografi
    and t1.fk_dim_alder = t2.mottaker_fk_dim_alder
    and t1.fk_dim_kjonn = t2.mottaker_fk_dim_kjonn
)

select final.*
,localtimestamp as lastet_dato  
from final