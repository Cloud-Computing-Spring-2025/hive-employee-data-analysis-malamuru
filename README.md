# HadoopHiveHue
Hadoop , Hive, Hue setup pseudo distributed  environment  using docker compose

# Hive Assignment: Employee and Department Data Analysis

## **Objective**
Analyze employee and department data using Hive by executing queries on a partitioned table, ensuring correct data loading using the `ALTER TABLE` statement rather than specifying partitions at creation.

## **Setup Instructions**
### **1. Start Hive**
Run the following command in your VS Code terminal:
```bash
docker compose up -d

docker cp input_dataset/employees.csv hive-server:/opt/

docker cp input_dataset/departments.csv  hive-server:/opt/

docker cp queries.hql  hive-server:/opt/

docker exec -it hive-server /bin/bash

hdfs dfs -mkdir/ input

hdfs dfs -put departments.csv /input_dataset/

hdfs dfs -put employees.csv /input_dataset/

```

### **2. Create and Load Tables**
#### **Create Temporary Employees Table**
```sql
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
STORED AS TEXTFILE;
```
#### **Load Employees Data**
```sql
LOAD DATA INPATH '/input_dataset/employees.csv' INTO TABLE temp_employees;
```

#### **Create Departments Table**
```sql
CREATE TABLE departments (
    dept_id INT,
    department_name STRING,
    location STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
```
#### **Load Departments Data**
```sql
LOAD DATA INPATH '/input_dataset/departments.csv' INTO TABLE departments;
```

### **3. Create Employees Table with Partitioning**
```sql
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
```

#### **Inserting Data into Partitioned Table**
```sql

ALTER TABLE employees ADD PARTITION (department='HR');
ALTER TABLE employees ADD PARTITION (department='IT');
ALTER TABLE employees ADD PARTITION (department='Finance');
ALTER TABLE employees ADD PARTITION (department='Marketing');
ALTER TABLE employees ADD PARTITION (department='Operations');
```

```sql

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
```


#### **Check Partitions**
```sql
SHOW PARTITIONS employees;
```

## **Query Execution in Hue**
Once the data is loaded, running these queries in Hue:

### **1. Retrieve all employees who joined after 2015**
```sql
SELECT * FROM employees WHERE year(join_date) > 2015;
```

### **2. Find the average salary of employees in each department**
```sql
SELECT department, AVG(salary) AS average_salary FROM employees GROUP BY department;
```

### **3. Identify employees working on the 'Alpha' project**
```sql
SELECT * FROM employees WHERE project = 'Alpha';
```

### **4. Count the number of employees in each job role**
```sql
SELECT job_role, COUNT(*) AS count FROM employees GROUP BY job_role;
```

### **5. Retrieve employees whose salary is above the average salary of their department**
```sql
SELECT emp1.* FROM employees emp1 
JOIN (SELECT department, AVG(salary) AS avg_salary FROM employees GROUP BY department) emp2 
ON emp1.department = emp2.department 
WHERE emp1.salary > emp2.avg_salary;
```

### **6. Find the department with the highest number of employees**
```sql
SELECT department, COUNT(*) AS employee_count FROM employees 
GROUP BY department ORDER BY employee_count DESC LIMIT 1;
```

### **7. Check for employees with null values in any column**
```sql
SELECT * FROM employees WHERE emp_id IS NULL OR name IS NULL OR age IS NULL OR 
job_role IS NULL OR salary IS NULL OR project IS NULL OR join_date IS NULL OR department IS NULL;
```

### **8. Join employees and departments to display employee details along with department locations**
```sql
SELECT e.emp_id, e.name, e.age, e.job_role, e.salary, e.project, e.join_date, 
       d.department_name, d.location
FROM employees e 
JOIN departments d ON e.department = d.department_name;
```

### **9. Rank employees within each department based on salary**
```sql
SELECT emp.emp_id, emp.name, emp.age, emp.job_role, emp.salary, emp.project, emp.join_date, 
       dept.department_name, dept.location
FROM employees emp
JOIN departments dept ON emp.department = dept.department_name;
```

### **10. Find the top 3 highest-paid employees in each department**
```sql
SELECT * FROM (
    SELECT emp_id, name, salary, department,
           DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
    FROM employees
) ranked WHERE rank <= 3;
```
### **4. Printing Output to File**

beeline -u jdbc:hive2://localhost:10000 -f queries.hql --outputformat=table > output.txt

docker cp  hive-server:/opt/output.txt Output/

### **5. Push to GitHub**
