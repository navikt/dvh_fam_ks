with ks_fagsak as (
    select behandlings_id, pk_ks_fagsak, kafka_offset
    from {{ref ('int_ks_fagsak')}}
),

utbetaling as (
    select *
    from {{ref ('stg_ks_utbetaling')}}
),

final as (
    select
        --TO_NUMBER(REPLACE(u.behandlings_id || u.kafka_offset || TO_CHAR(stonad_fom, 'YYYYMMDD') || TO_CHAR(stonad_tom, 'YYYYMMDD'), '-', '')) AS pk_ks_utbetaling,
        {{ dbt_utils.generate_surrogate_key(['u.behandlings_id', 'u.kafka_offset', 'u.stonad_fom', 'u.stonad_tom']) }} as pk_ks_utbetaling,
        --to_number(replace(u.behandlings_id || TO_CHAR(stonad_fom, 'YYYYMMDD')  || TO_CHAR(stonad_tom, 'YYYYMMDD') , '-', '')) as pk_ks_utbetaling,
        --STANDARD_HASH(u.behandlings_id || u.kafka_offset || NVL(TO_CHAR(stonad_fom, 'YYYYMMDD'), 'x')|| NVL(TO_CHAR(stonad_tom, 'YYYYMMDD'), 'x'), 'MD5') as pk_ks_utbetaling_ny,
        u.kafka_offset,
        hjemmel,
        utbetalt_per_mnd,
        stonad_fom,
        stonad_tom,
        f.pk_ks_fagsak as fk_ks_fagsak
    from utbetaling u
    join ks_fagsak f
    on u.kafka_offset = f.kafka_offset
    and u.behandlings_id = f.behandlings_id
)

select *
from final