import sqlite3
import pandas as pd
conn = sqlite3.connect('emp_dbms.db')
cursor = conn.cursor()

cursor.execute("""
drop table employees;
""")
conn.commit()

cursor.execute("""
create table if not exists employees ( 
emp_id integer primary key,
name text not null, 
department text, 
salary integer not null, 
years_of_service double not null);
""")

conn.commit()
print("Table created successfully")

df = pd.read_csv('employees.csv')
df.to_sql('employees', conn, if_exists='replace', index=False)
print("Data inserted successfully")


def insert_employee(emp_id, name, department, salary, years_of_service):
    cursor.execute("""
    insert into employees (emp_id, name, department, salary, years_of_service) 
    values (?, ?, ?, ?, ?)
    """, (emp_id, name, department, salary, years_of_service))
    conn.commit()
    print("Data inserted successfully")

def select_employees(department):
    cursor.execute("""
    select * from employees where department = ?
    """, (department,))
    rows = cursor.fetchall()
    for row in rows:
        print(row)

def modify_employee(emp_id, col, val):
    query = f"UPDATE employees SET {col} = '{val}' WHERE emp_id = {emp_id}"
    cursor.execute(query)
    conn.commit()
    print("Data updated successfully!")

def delete_employee(emp_id):
    query = f"DELETE FROM employees WHERE emp_id = {emp_id}"
    cursor.execute(query)
    conn.commit()
    print("Data deleted successfully!")

def display_all_employees():
    cursor.execute("SELECT * FROM employees")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

insert_employee(1011,"Alex", "IT",10000,2)
insert_employee(1012,"John", "HR",5000,4)
select_employees("HR")
modify_employee(1011,"department","MR")
delete_employee(1002)
display_all_employees()

#Point No 2 (Use SQL JOIN to connect `employees` and `departments` tables)
department_create_query = (f"""
    create table if not exists departments ( 
    dept_id text primary key,
    dept_name text not null);""")
cursor.execute(department_create_query)
conn.commit()
print("Department table created successfully")

insert_query = (f"""
    INSERT OR IGNORE INTO departments (dept_id, dept_name) VALUES
    ('HR', 'Human Resource'),
    ('FN', 'Finance'),
    ('IT', 'Information Technology'),
    ('MR', 'Marketing'),
    ('OP', 'Operations');
    """)
cursor.execute(insert_query)
conn.commit()
print("Data inserted successfully")

emp_dep_join_query = (f"""
    select * from employees join departments on employees.department = departments.dept_id 
""")

cursor.execute(emp_dep_join_query)
rows = cursor.fetchall()
for row in rows:
    print(row)

#Calculate average salary by department using GROUP BY

avg_salary_query = (f"""
    select department,avg(salary) from employees 
    group by department;
""")
cursor.execute(avg_salary_query)
rows = cursor.fetchall()
for row in rows:
    print(row)

#Give 10% increment using SQL UPDATE for employees with >5 years of service

salary_increment_query = (f"""
    update employees set salary = salary * 1.1
    where years_of_service>5
""")
cursor.execute(salary_increment_query)
conn.commit()

#Export department-wise reports to CSV files
query = """
SELECT
    e.emp_id,
    e.name,
    e.salary,
    e.years_of_service,
    d.dept_id,
    d.dept_name
FROM employees e
JOIN departments d
ON e.department = d.dept_id
"""

df = pd.read_sql(query, conn)

for dept_id,group in df.groupby('dept_id'):
    dept_name = group['dept_name'].iloc[0].replace(" ","_")
    group.to_csv(f"{dept_id}_{dept_name}.csv", index=False)
    print(f"exported {dept_id}_{dept_name}.csv")

conn.close()







