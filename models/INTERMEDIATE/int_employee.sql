with joins as (

    select
        employee_id,
        hire_date,
        employee_name,
        city,
        address,
        title,
        annual_salary
    from {{ ref('GOOGLEDRIVE_HRJOINS') }}

),

quits as (

    select
        employee_id,
        quit_date
    from {{ ref('GOOGLEDRIVE_HRQUITS') }}

)

select
    j.employee_id,
    j.employee_name,
    j.city,
    j.address,
    j.title,
    j.annual_salary,
    j.hire_date,
    q.quit_date,
    case
        when q.employee_id is null then 'active'
        else 'quit'
    end as employment_status
from joins j
left join quits q
    on j.employee_id = q.employee_id