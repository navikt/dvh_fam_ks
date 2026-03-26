{{
    config(
        materialized='incremental'
    )
}}

with kontanststotte_meta_data as (
  select * from {{ref ('ks_meldinger_til_aa_pakke_ut')}}
),

pre_final as (
select * from kontanststotte_meta_data,
  json_table(melding, '$'
    COLUMNS (
      behandlings_id      NUMBER(38,0) PATH '$.behandlingsId',
      NESTED              PATH '$.kompetanseperioder[*]'
      COLUMNS (
         tom                                         VARCHAR2(255) PATH '$.tom'
        ,fom                                         VARCHAR2(255) PATH '$.fom'
        ,kompetanse_aktivitet                        VARCHAR2(255) PATH  '$.kompetanseAktivitet'
        ,kompetanse_Resultat                         VARCHAR2(255) PATH '$.resultat'
        ,barnets_bostedsland                         VARCHAR2(255) PATH '$.barnetsBostedsland'
        ,SOKERS_AKTIVITETSLAND                       VARCHAR2(255) PATH '$.sokersAktivitetsland'
        ,ANNEN_FORELDERS_AKTIVITET                   VARCHAR2(255) PATH '$.annenForeldersAktivitet'
        ,ANNEN_FORELDERS_AKTIVITETSLAND              VARCHAR2(255) PATH '$.annenForeldersAktivitetsland'
        ,ANNEN_FORELDER_OMFATTET_AV_NORSK_LOVGIVNING VARCHAR2(255) PATH '$.annenForelderOmfattetAvNorskLovgivning'
    ))
    )j
    where json_value (melding, '$.kompetanseperioder.size()' )> 0
  ),

final as (
  select
  behandlings_id as fk_ks_fagsak,
  TO_CHAR(TO_DATE(fom, 'YYYY-MM'), 'YYYYMM'),
  TO_CHAR(TO_DATE(tom, 'YYYY-MM'), 'YYYYMM'),
  kompetanse_aktivitet,
  kompetanse_Resultat,
  barnets_bostedsland,
  kafka_offset,
  SOKERS_AKTIVITETSLAND,
  ANNEN_FORELDERS_AKTIVITET,
  ANNEN_FORELDERS_AKTIVITETSLAND,
  CASE 
    WHEN ANNEN_FORELDER_OMFATTET_AV_NORSK_LOVGIVNING = 'false' then 0
    ELSE 1
  END ANNEN_FORELDER_OMFATTET_AV_NORSK_LOVGIVNING
  from pre_final
)

select
  dvh_fam_ks.hibernate_sequence.nextval as PK_KS_KOMPETANSE_PERIODER
  ,FOM
  ,TOM
  ,FK_KS_FAGSAK
  ,KOMPETANSE_AKTIVITET
  ,SOKERS_AKTIVITETSLAND
  ,ANNEN_FORELDERS_AKTIVITET
  ,ANNEN_FORELDERS_AKTIVITETSLAND
  ,BARNETS_BOSTEDSLAND
  ,kompetanse_Resultat
  ,ANNEN_FORELDER_OMFATTET_AV_NORSK_LOVGIVNING
  ,localtimestamp as LASTET_DATO
  ,kafka_offset
from final


