select
    EMPLOYEE_ID,
    QUIT_DATE

from {{ source('google_drive', 'HR_QUITS') }}