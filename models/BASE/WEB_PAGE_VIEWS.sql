select
    page_name,
    session_id,
    view_at,
    "_fivetran_deleted",
    "_fivetran_id",
    "_fivetran_synced"
from {{ source("web_schema", "PAGE_VIEWS") }}