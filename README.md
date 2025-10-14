# mdm_preparing_bo_mapping_plpgsql
Mapping business object PL/pgSQL syntax

Start 

1. Change environment and credentials into creds.py
For example: Path env, DataBase credentials 

2. Change array into bo_mapping.py 
arr_rellation = ['schema.table'] #list type: schema.table 
arr_file = ['select.txt'] #list type: for example, ['select.txt','insert.txt',update.txt', 'var.txt']

3. Start bo_mapping.py

Output

-------------------------------
-- insert BO table_name
<sql clause = "
DECLARE
   n_id uuid := uuid(:v_id);
   n_code character varying := :v_code;
   n_sname jsonb := :v_sname;
   n_fname jsonb := :v_fname;
   n_is_archive boolean := :v_is_archive;

BEGIN
INSERT INTO schema.table (
   id
   ,code
   ,sname
   ,fname
   ,is_archive
)
values (
   n_id
   ,n_code
   ,n_sname
   ,n_fname
   ,n_is_archive

   );
END;
">
</sql>

Algorithms

1.Take relation:
SELECT                   
schemaname,
relname
FROM pg_catalog.pg_statio_all_tables

2. Take structure: 
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns

3. Transform array to PL/pgSQL DML query:select, insert, update and var.

