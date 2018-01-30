SELECT department.department_name, avg(vacation_days) "average_vacation_days"
FROM compensation
INNER JOIN professor on professor.id = compensation.professor_id
INNER JOIN department on department.id = professor.department_id
GROUP BY department.department_name
ORDER BY "average_vacation_days";
