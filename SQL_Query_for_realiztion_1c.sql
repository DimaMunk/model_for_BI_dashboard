with query_1 as
(
select sum(cost_usd) as sum_usd,department_id as department
from f_realization_1c
join s_calendar on f_realization_1c.date_id = s_calendar.date
where quarter = 3 AND year = '2015'
group by department_id
)
select sum_usd, department_name 
from query_1 join s_department on department = s_department.id
