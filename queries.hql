set hive.resultset.use.unique.column.names=false;

-- Create Temporary Table for Employees
CREATE TABLE temp_employees (
    emp_id INT,
    name STRING,
    age INT,
    job_role STRING,
    salary FLOAT,
    project STRING,
    join_date STRING,
    department STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");


-- Load Data into Temporary Table
LOAD DATA INPATH '/input_dataset/employees.csv' INTO TABLE temp_employees;

-- Create Departments Table
CREATE TABLE departments (
    dept_id INT,
    department_name STRING,
    location STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- Load Data into Departments Table
LOAD DATA INPATH '/input_dataset/departments.csv' INTO TABLE departments;

-- Create Partitioned Employees Table
CREATE TABLE employees (
    emp_id INT,
    name STRING,
    age INT,
    job_role STRING,
    salary FLOAT,
    project STRING,
    join_date STRING
)
PARTITIONED BY (department STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Insert Data into Partitioned Table
ALTER TABLE employees ADD PARTITION (department='HR');
ALTER TABLE employees ADD PARTITION (department='IT');
ALTER TABLE employees ADD PARTITION (department='Finance');
ALTER TABLE employees ADD PARTITION (department='Marketing');
ALTER TABLE employees ADD PARTITION (department='Operations');


INSERT overwrite TABLE employees PARTITION(department='HR')
SELECT emp_id, name, age, job_role, salary, project, join_date FROM temp_employees where department='HR';

INSERT overwrite TABLE employees PARTITION(department='IT')
SELECT emp_id, name, age, job_role, salary, project, join_date FROM temp_employees where department='IT';

INSERT overwrite TABLE employees PARTITION(department='Finance')
SELECT emp_id, name, age, job_role, salary, project, join_date FROM temp_employees where department='Finance';

INSERT overwrite TABLE employees PARTITION(department='Marketing')
SELECT emp_id, name, age, job_role, salary, project, join_date FROM temp_employees where department='Marketing';

INSERT overwrite TABLE employees PARTITION(department='Operations')
SELECT emp_id, name, age, job_role, salary, project, join_date FROM temp_employees where department='Operations';

-- Check Partitions
SHOW PARTITIONS employees;

-- Queries for Analysis

-- 1. Retrieve all employees who joined after 2015
SELECT * FROM employees WHERE year(join_date) > 2015;

-- 2. Find the average salary of employees in each department
SELECT department, AVG(salary) AS average_salary FROM employees GROUP BY department;

-- 3. Identify employees working on the 'Alpha' project
SELECT * FROM employees WHERE project = 'Alpha';

-- 4. Count the number of employees in each job role
SELECT job_role, COUNT(*) AS count FROM employees GROUP BY job_role;

-- 5. Retrieve employees whose salary is above the average salary of their department
SELECT emp1.* FROM employees emp1 
JOIN (SELECT department, AVG(salary) AS avg_salary FROM employees GROUP BY department) emp2 
ON emp1.department = emp2.department 
WHERE emp1.salary > emp2.avg_salary;

-- 6. Find the department with the highest number of employees
SELECT department, COUNT(*) AS employee_count FROM employees 
GROUP BY department ORDER BY employee_count DESC LIMIT 1;

-- 7. Check for employees with null values in any column
SELECT * FROM employees WHERE emp_id IS NULL OR name IS NULL OR age IS NULL OR 
job_role IS NULL OR salary IS NULL OR project IS NULL OR join_date IS NULL OR department IS NULL;

-- 8. Join employees and departments to display employee details along with department locations
SELECT emp.emp_id, emp.name, emp.age, emp.job_role, emp.salary, emp.project, emp.join_date, 
       dept.department_name, dept.location
FROM employees emp
JOIN departments dept ON emp.department = dept.department_name;

-- 9. Rank employees within each department based on salary
SELECT emp_id, name, salary, department, RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS salary_rank
FROM employees;

-- 10. Find the top 3 highest-paid employees in each department
SELECT * FROM (
    SELECT emp_id, name, salary, department,
        DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
    FROM employees
) ranked WHERE rank <= 3;
