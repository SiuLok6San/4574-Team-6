select
    city,
    count(*) as total_employees,
    sum(case when employment_status = 'active' then 1 else 0 end) as active_employees,
    sum(case when employment_status = 'quit' then 1 else 0 end) as quit_employees
from {{ ref('dim_employee') }}
where city is not null
group by 1
order by total_employees desc