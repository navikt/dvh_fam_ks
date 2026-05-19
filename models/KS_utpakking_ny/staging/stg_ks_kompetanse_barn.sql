with ks_meta_data as (
  select * from {{ref ('stg_ks_meta_data')}}
),

pre_final as (
select * from ks_meta_data,
  json_table(melding, '$'
    COLUMNS (
        behandlings_id                                  NUMBER(38,0)  PATH '$.behandlingsId',
        nested path '$.kompetanseperioder[*]'
        COLUMNS (
            tom                                         VARCHAR2(255) PATH '$.tom'
            ,fom                                        VARCHAR2(255) PATH '$.fom'
            ,kompetanse_aktivitet                       VARCHAR2(255) PATH '$.kompetanseAktivitet'
            ,kompetanse_Resultat                        VARCHAR2(255) PATH '$.resultat'
            ,barnets_bostedsland                        VARCHAR2(255) PATH '$.barnetsBostedsland'
            ,sokers_aktivitetsland                       VARCHAR2(255) PATH '$.sokersAktivitetsland'
            ,annen_forelders_aktivitet                    VARCHAR2(255) PATH '$.annenForeldersAktivitet'
            ,annen_forelders_aktivitetsland               VARCHAR2(255) PATH '$.annenForeldersAktivitetsland'
            ,annen_forelder_omfattet_av_norsk_lovgivning     VARCHAR2(255) PATH '$.annenForelderOmfattetAvNorskLovgivning'
            ,nested path '$.barnsIdenter[*]'
            columns (
                personidentbarn                         VARCHAR2(255) PATH '$[*]'
                )
            )
        )
    ) j
    where json_value (melding, '$.kompetanseperioder.size()' )> 0
),

final as (
    select 
        behandlings_id,
        fom,
        tom,
        kompetanse_aktivitet,
        kompetanse_Resultat,
        barnets_bostedsland,
        sokers_aktivitetsland,
        annen_forelders_aktivitet,
        annen_forelders_aktivitetsland,
        annen_forelder_omfattet_av_norsk_lovgivning,
        personidentbarn,
        kafka_offset,
        kafka_mottatt_dato
    from pre_final
)

select * from final