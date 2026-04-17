select*
    client_id,
    ip,
    os,
    session_at,
    session_id,
    _fivetran_deleted,
    _fivetran_id,
    _fivetran_synced

from {{ source('web_schema', 'SESSIONS') }}