# mdm_preparing_bo_mapping_plpgsql
Mapping business object PL/pgSQL syntax

Start
-------------------------------

1. Install lib:pandas, psycorg2
2. Change environment and credentials into creds.py
For example: Path env, DataBase credentials
3. Change array into file bo_mapping.py by add entities 
arr_rellation = ['schema.table'];
arr_file = ['select.txt', 'insert.txt', update.txt', 'var.txt'];
5. Start bo_mapping.py;

Output
-------------------------------

insert.txt

Algorithms
-------------------------------
1. Take relation:
SELECT                   
schemaname,
relname
FROM pg_catalog.pg_statio_all_tables
2. Take structure: 
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
3. Transform array to PL/pgSQL DML-query:select, insert, update and var.

