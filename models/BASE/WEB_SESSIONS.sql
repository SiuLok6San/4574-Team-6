select
    client_id,
    ip,
    os,
    session_at,
    session_id,
<<<<<<< HEAD
    "_fivetran_id",
    "_fivetran_deleted",
    "_fivetran_synced"
=======
    "_fivetran_deleted" as _fivetran_deleted,
    "_fivetran_id" as _fivetran_id,
    "_fivetran_synced" as _fivetran_synced
>>>>>>> dd294406af7b40420bfffad475cd3bfb05c0d8a0
from {{ source('web_schema', 'SESSIONS') }}