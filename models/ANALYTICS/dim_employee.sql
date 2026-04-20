select
    employee_id,
    employee_name,
    city,
    address,
    title,
    annual_salary,
    hire_date,
    quit_date,
    employment_status
from {{ ref('int_employee') }}