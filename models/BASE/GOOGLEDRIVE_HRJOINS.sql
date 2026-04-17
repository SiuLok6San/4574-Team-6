select
    EMPLOYEE_ID,
    HIRE_DATE,
    NAME,
    CITY,
    ADDRESS,
    TITLE,
    ANNUAL_SALARY

from {{ source('google_drive', 'HR_JOINS') }}