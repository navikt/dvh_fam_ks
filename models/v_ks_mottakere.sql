with ks_infotrygd as (
  select distinct FK_DIM_TID_MND as fk_dim_tid
    ,FK_DIM_GEOGRAFI_BOSTED
    ,FK_DIM_ALDER
    ,FK_DIM_KJONN
    ,FK_PERSON1_MOTTAKER
  from  {{ source ('fam_ks', 'fak_ks_mottaker_infotrygd') }}
  where FK_DIM_TID_MND is not null 
  and FK_DIM_ALDER is not null
  and belopm > 0
),

ks as (
  select distinct FK_DIM_TID_MND as fk_dim_tid
    ,FK_DIM_GEOGRAFI_BOSTED
    ,FK_DIM_ALDER
    ,FK_DIM_KJONN
    ,FK_PERSON1_MOTTAKER
  from  {{ source ('fam_ks', 'FAK_KS_MOTTAKER') }}
  where belopm > 0
),

final as (
    select * from ks_infotrygd

    UNION

    select * from ks

)

select * from final

