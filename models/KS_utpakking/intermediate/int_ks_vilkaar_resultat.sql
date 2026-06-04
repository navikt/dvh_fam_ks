with ks_fagsak as (
    select behandlings_id, pk_ks_fagsak, kafka_offset
    from {{ref ('int_ks_fagsak')}}
),

vilkaar_resultat as (
    select *
    from {{ref ('stg_ks_vilkaar_resultat')}}
),

final as (
    select
        behandlings_id,
        resultat,
        ident,
        antall_timer,
        periode_fom,
        periode_tom,
        ROW_NUMBER() OVER (PARTITION BY behandlings_id, kafka_offset, NVL(b.fk_person1, -1) ORDER BY periode_fom) AS rn,
        kafka_offset,
        nvl(b.fk_person1, -1) fk_person1,
        vilkaar_type
    from vilkaar_resultat v
    left outer join {{ source('dt_person', 'ident_off_id_til_fk_person1') }} b
    on v.ident = b.off_id
    and v.kafka_mottatt_dato between b.gyldig_fra_dato and b.gyldig_til_dato
    and b.skjermet_kode = 0
)

select
    {{ dbt_utils.generate_surrogate_key(['f.behandlings_id', 'fk_person1', 'f.kafka_offset' ,'rn', 'f.periode_fom', 'f.periode_tom', 'f.vilkaar_type']) }}  as pk_ks_vilkaar_resultat,
    --STANDARD_HASH(f.behandlings_id || fk_person1 || f.kafka_offset|| rn || NVL(TO_CHAR(f.periode_fom, 'YYYYMMDD'), 'x') || NVL(TO_CHAR(f.periode_tom, 'YYYYMMDD'), 'x') || NVL(f.vilkaar_type, '-'), 'MD5') AS pk_ks_vilkaar_resultat,
    resultat,
    antall_timer,
    periode_fom,
    periode_tom,
    f.kafka_offset,
    case when fk_person1 = -1 then ident
        else cast(null as varchar2(11))
    end ident,
    fk_person1,
    vilkaar_type,
    k.pk_ks_fagsak as fk_ks_fagsak
from final f
join ks_fagsak k
on  f.kafka_offset = k.kafka_offset
and f.behandlings_id = k.behandlings_id