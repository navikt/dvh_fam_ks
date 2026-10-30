{{ config(materialized='ephemeral') }}

select 
    t1.pk_dim_tid as fk_dim_tid
    ,t1.aar
    ,t1.kvartal_besk
    ,replace(t1.aar_kvartal_besk_kort, 'k0', 'K' ) as aar_kvartal_besk_kort
    ,t1.aar_kvartal_besk
    ,t1.aar_halvaar_besk_kort
    ,t1.aar_halvaar_besk
    ,t2.pk_dim_geografi as fk_dim_geografi
    ,t2.navarende_fylke_nr
    ,t2.navarende_fylke_navn
    ,t2.navarende_fylke_nr_navn
    ,t2.navarende_kommune_nr
    ,t2.navarende_kommune_navn
    ,t2.navarende_kommune_nr_navn
    ,t2.bydel_nr
    ,t2.bydel_navn
    ,t2.bydel_nr_navn
    ,t3.pk_dim_kjonn as fk_dim_kjonn
    ,t3.kjonn_kode
    ,t3.kjonn_flertall_besk
    ,t4.pk_dim_alder as fk_dim_alder
    ,t4.alder
    ,t4.alder_gruppe1_besk
    ,t4.ti_aar_gruppe_besk
    ,t4.alder_gruppe5_besk
    ,t4.alder_gruppe7_besk
from {{ source ('kode_verk', 'dim_tid') }} t1
JOIN {{ source ('kode_verk', 'dim_geografi') }} t2 
on t1.siste_dato_i_perioden between t2.funk_gyldig_fra_dato and t2.funk_gyldig_til_dato
CROSS JOIN {{ source ('kode_verk', 'dim_kjonn') }} t3
CROSS JOIN {{ source ('kode_verk', 'dim_alder') }} t4
where t1.gyldig_flagg = 1
and t1.dim_nivaa = 3
and t1.maaned in (3,6,9,12)
and t1.aar >= '2018'and t1.aar <= TO_CHAR(SYSDATE, 'YYYY')
and t2.gyldig_flagg = 1 -- gir ingen forskjell i sum av ks mottakere for årene 2013-2025
and t2.dim_nivaa != 1 -- Fjerner poststednivå (gammelt)
and t3.gyldig_flagg = 1
and t3.dim_nivaa = 1  
and t4.gyldig_flagg = 1

