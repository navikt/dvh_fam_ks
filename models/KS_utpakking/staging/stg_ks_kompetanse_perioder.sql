with ks_meta_data as (
  select * from {{ref ('stg_ks_meta_data')}}
),

pre_final as (
select * from ks_meta_data,
  json_table(melding, '$'
    COLUMNS (
      behandlings_id                                NUMBER(38,0)    PATH '$.behandlingsId',
      NESTED                                                        PATH '$.kompetanseperioder[*]'
      COLUMNS (
         tom                                         VARCHAR2(255)  PATH '$.tom'
        ,fom                                         VARCHAR2(255)  PATH '$.fom'
        ,kompetanse_aktivitet                        VARCHAR2(255)  PATH  '$.kompetanseAktivitet'
        ,kompetanse_Resultat                         VARCHAR2(255)  PATH '$.resultat'
        ,barnets_bostedsland                         VARCHAR2(255)  PATH '$.barnetsBostedsland'
        ,SOKERS_AKTIVITETSLAND                       VARCHAR2(255)  PATH '$.sokersAktivitetsland'
        ,ANNEN_FORELDERS_AKTIVITET                   VARCHAR2(255)  PATH '$.annenForeldersAktivitet'
        ,ANNEN_FORELDERS_AKTIVITETSLAND              VARCHAR2(255)  PATH '$.annenForeldersAktivitetsland'
        ,ANNEN_FORELDER_OMFATTET_AV_NORSK_LOVGIVNING VARCHAR2(255)  PATH '$.annenForelderOmfattetAvNorskLovgivning'
    ))
    )j
    where json_value (melding, '$.kompetanseperioder.size()' )> 0
  ),

final as (
    select 
        behandlings_id,
        tom,
        fom,
        kompetanse_aktivitet,
        kompetanse_Resultat,
        barnets_bostedsland,
        SOKERS_AKTIVITETSLAND,
        ANNEN_FORELDERS_AKTIVITET,
        ANNEN_FORELDERS_AKTIVITETSLAND,
        ANNEN_FORELDER_OMFATTET_AV_NORSK_LOVGIVNING,
        kafka_offset
    from pre_final
)

select * from final