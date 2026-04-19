select
    _file,
    _line,
    _modified as modified_ts,
    _fivetran_synced as fivetran_synced_ts,
    date as expense_date,
    trim(expense_type) as expense_type,
    try_cast(
        trim(replace(expense_amount, '$', '')) as number(38,2)
    ) as expense_amount
from {{ source('google_drive', 'EXPENSES') }}
