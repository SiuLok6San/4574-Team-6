select
    client_id,
    ip,
    os,
    try_to_timestamp(session_at) as session_at,
    session_id,
    "_fivetran_deleted" as _fivetran_deleted,
    "_fivetran_id" as _fivetran_id,
    "_fivetran_synced" as _fivetran_synced
from {{ source('web_schema', 'SESSIONS') }}