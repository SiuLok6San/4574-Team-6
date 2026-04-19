select distinct
    client_id
from {{ ref('int_sessions') }}
where client_id is not null