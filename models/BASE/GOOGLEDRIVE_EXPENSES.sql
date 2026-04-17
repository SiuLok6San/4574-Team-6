select
    date as expense_date,
    expense_amount,
    expense_type
from {{ source('google_drive', 'EXPENSES') }}