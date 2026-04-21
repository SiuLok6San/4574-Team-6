-- Creates daily employee movement and net change by day.
with employee_status as (

    select
        employee_id,
        hire_date,
        quit_date,
        employment_status
    from {{ ref('int_employee') }}

),

hire_days as (

    select
        hire_date as date_day,
        count(*) as hired_count
    from employee_status
    where hire_date is not null
    group by 1

),

quit_days as (

    select
        quit_date as date_day,
        count(*) as quit_count
    from employee_status
    where quit_date is not null
    group by 1

)

select
    coalesce(h.date_day, q.date_day) as date_day,
    coalesce(h.hired_count, 0) as hired_count,
    coalesce(q.quit_count, 0) as quit_count,
    coalesce(h.hired_count, 0) - coalesce(q.quit_count, 0) as net_employee_change
from hire_days h
full outer join quit_days q
    on h.date_day = q.date_day
order by 1