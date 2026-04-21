with ranked as (

    select
        *,
        row_number() over (
            partition by session_id
            order by session_at asc
        ) as rn
    from {{ ref('WEB_SESSIONS') }}
    where session_id is not null

)

select
    session_id,
    client_id,
    os,
    ip,
    session_at
from ranked
where rn = 1