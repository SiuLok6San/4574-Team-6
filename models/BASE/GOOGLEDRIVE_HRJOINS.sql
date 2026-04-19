select
    _file,
    _line,
    _modified as modified_ts,
    _fivetran_synced as fivetran_synced_ts,
    employee_id,
    try_to_date(
        regexp_replace(lower(trim(hire_date)), '[^0-9-]', ''),
        'YYYY-MM-DD'
    ) as hire_date,
    trim(name) as employee_name,
    trim(city) as city,
    trim(address) as address,
    trim(title) as title,
    annual_salary
from {{ source('google_drive', 'HR_JOINS') }}
