{{
    config(
        materialized='table',
        post_hook="{{ dbt_dvh_macros.BREDAGG__sync_multi_source_comments([ ['kode_verk', 'dim_tid'], 
		['kode_verk', 'dim_geografi'], ['kode_verk', 'dim_alder'],
        ['kode_verk', 'dim_kjonn']]) }}"
    )
}}

    with mot as (
    select FK_DIM_TID_MND as fk_dim_tid
    ,FK_DIM_GEOGRAFI_BOSTED
    ,FK_DIM_ALDER
    ,FK_DIM_KJONN
    ,count(distinct FK_PERSON1_MOTTAKER) as kontantstotte_antall_mottakere
    from {{ source ('fam_ks', 'FAK_KS_MOTTAKER') }} t1
    left join {{ source ('kode_verk', 'dim_tid') }} t2
    on t1.FK_DIM_TID_MND = t2.PK_DIM_TID
    where t2.maaned in (3,6,9,12)
    group by FK_DIM_TID_MND
    ,FK_DIM_GEOGRAFI_BOSTED
    ,FK_DIM_ALDER
    ,FK_DIM_KJONN
),


final as ( 
    select 
    'Kontantstøtte' as kilde_omraade,
    {{ dbt_dvh_macros.BREDAGG__ephemeral_star(model_name='dim_skjelett_kvartal', relation_alias='t1') }},
    t2.kontantstotte_antall_mottakere
    from {{ ref('dim_skjelett_kvartal') }} t1
    left join mot t2
    on t1.fk_dim_tid = t2.fk_dim_tid
    and t1.fk_dim_geografi = t2.FK_DIM_GEOGRAFI_BOSTED
    and t1.fk_dim_alder = t2.fk_dim_alder
    and t1.fk_dim_kjonn = t2.fk_dim_kjonn
)

select final.*
,localtimestamp as lastet_dato  
from final