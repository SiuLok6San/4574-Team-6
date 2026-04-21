with ranked as (

    select
        client_id,
        session_at,
        os,
        ip,
        row_number() over (
            partition by client_id
            order by session_at desc
        ) as rn
    from {{ ref('int_sessions') }}
    where client_id is not null

)

select
    client_id,
    session_at as latest_session_at,
    os as latest_os,
    ip as latest_ip
from ranked
where rn = 1