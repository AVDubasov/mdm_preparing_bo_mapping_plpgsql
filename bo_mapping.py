#!/usr/bin/env python3

import function_mapping as func
import pandas as pd
import creds as cred

arr_rellation = ['schema.table'] #list: schema.table 
file = ['select.txt'] #list: files

df_file = pd.DataFrame({'file':file})

# get db object matrix
df_db = func.get_db(arr_rellation)


# make sure indexes pair with number of rows
df_db = df_db.reset_index()

for index_file, row_file in df_file.iterrows():
    
    func.clear_txt(path_output,row_file['file']) #clear file
    
    for index, row in df_db.iterrows():

        db = row['db']
        schema = row['schema']
        table = row['table']  
    
        sql_sch_tbl =  """SELECT                   
                        schemaname,
                        relname
                        FROM pg_catalog.pg_statio_all_tables
                        WHERE schemaname != 'pg_catalog' 
                            AND schemaname != 'information_schema' 
                            AND schemaname = '{}'
                            AND relname = '{}';
                    """.format(schema,table)  
        
        sql_columns = """SELECT column_name, data_type, character_maximum_length
                        FROM information_schema.columns
                        WHERE table_schema = '{}'
                        AND table_name   = '{}'
                        AND column_name !~ '^(create_|update_|attrib).*';
                    """.format(schema,table)
        """                  
        Uses get_tables and get_columns to create a tree-like data
        structure of tables and columns.

        It is not a true tree but a list of dictionaries containing
        tables, each dictionary having a second dictionary
        containing column information.
        """
        
        connection = func.connection_func(db, cred.server, cred.usr, cred.pwd, cred.prt)
        
        tree = func.get_tables(connection, sql_sch_tbl)

        for table in tree:

            table["columns"] = func.get_columns(connection, sql_columns)
               
        match row_file['file']:
            case 'select.txt': func.print_tree_slct(tree,path_output,row_file['file'])
      

