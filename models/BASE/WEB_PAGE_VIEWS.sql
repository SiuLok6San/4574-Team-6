select
    page_name,
    session_id,
    view_at,
    "_fivetran_deleted" as _fivetran_deleted,
    "_fivetran_id" as _fivetran_id,
    "_fivetran_synced" as _fivetran_synced
from {{ source('web_schema', 'PAGE_VIEWS') }}