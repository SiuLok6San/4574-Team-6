select
    _file,
    _line,
    _modified as modified_ts,
    _fivetran_synced as fivetran_synced_ts,
    employee_id,
    quit_date
from {{ source('google_drive', 'HR_QUITS') }}
