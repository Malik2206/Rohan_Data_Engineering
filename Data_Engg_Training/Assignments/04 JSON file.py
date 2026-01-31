import pandas as pd
import json
import sqlite3

#1.	Read JSON file containing database configuration settings
with open("config.json", "r") as read_file:
    config = json.load(read_file)

db_config = config["configurations"]["database"]

print(db_config)

#2.	Create `config_history` table to track configuration changes
conn = sqlite3.connect("config.db")
cursor = conn.cursor()

query = """
create table if not exists config_history (
    id integer primary key autoincrement,
    version text,
    config_data text,
    updated_at timestamp default current_timestamp)
"""

cursor.execute(query)
print("Table created successfully")
conn.commit()

#3.	Store each configuration version in SQLite with timestamps
config_str = json.dumps(config)
version = config["app_metadata"]["version"]

cursor.execute("""
insert into config_history (version,config_data)
values (?,?)
""",(version,config_str))
print("Data Inserted successfully")

conn.commit()

#4.	Query previous configurations using SQL SELECT with ORDER BY timestamp
cursor.execute("""
Select id,version,updated_at from config_history 
order by updated_at desc
""")

rows = cursor.fetchall()
for row in rows:
    print(row)

#5.	Validate required fields before updating database
required_fields = ["app_metadata","database","connection","credentials"]

for field in required_fields:
    if field not in config:
        print(f"Field: {field} not found in config")
        exit()

print("Fields found in config")

#6.	Export current configuration to JSON from database
cursor.execute("""
select config_data from config_history
order by updated_at desc
limit 1
""")
rows = cursor.fetchone()
config_json = json.loads(rows[0])
print(config_json)

#Export to JSON
with open("exported_config.json", "w") as write_file:
    json.dump(config_json, write_file, indent=4)

print("Current config is exported successfully")

#7.	Implement rollback functionality to restore previous configurations
rollback_version="1.2.0"











