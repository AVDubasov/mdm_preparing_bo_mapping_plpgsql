
import psycopg2
import psycopg2.extras
import pandas.io.sql as sqlio
import pandas as pd

def get_db(col_list):
     
     df = pd.DataFrame({'db_obj':col_list})
     df_main = df.assign(db_obj=df.db_obj.str.split("."))
     df_main = df_main.db_obj.apply(pd.Series) \
          .merge(df_main, right_index = True, left_index = True) \
          .drop(['db_obj'], axis = 1) \
          .rename(columns={0:'db',1:'schema',2:'table'})
     return df_main
     
def connection_func(postgres_db, server, usr, pwd, prt):

    connection = psycopg2.connect(database=postgres_db, user=usr, password=pwd, host=server, port=prt)  
    return connection

def get_tables(con_,sql_sch_tbl_):

    """
    Create and return a list of dictionaries with the
    schemas and names of tables in the database
    connected to by the connection argument.
    """

    cursor = con_.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    cursor.execute(sql_sch_tbl_)
    
    tables = cursor.fetchall()

    cursor.close()

    return tables

def get_columns(con_,sql_columns_):

    """
    Creates and returns a list of dictionaries for the specified
    schema.table in the database connected to.
    """
    cursor = con_.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute(sql_columns_)

    columns = cursor.fetchall()
    
    cursor.close()
    
    return columns

def clear_txt(path_output_,file_):
     
     file_=path_output_+file_
     with open(file_, 'r+') as f: f.truncate(0)

def print_tree_ins(tree_,path_output_,file_):
    
     """
     Prints the tree created by get_tree
     """
     file_=path_output_+file_
     with open(file_, 'a', encoding="utf-8") as f:
          for cortege in tree_:
               lines_1 = ["\n-------------------------------",
                         "-- insert BO {}",
                         "<sql clause = \"",
                         "DECLARE"]
               for line in lines_1:          
                    f.write(f"{line}\n".format(cortege["relname"]))     
               # assign var
               for i, column in enumerate(cortege["columns"]):
                    if column["data_type"] == 'uuid':
                         print("   n_{} {} := uuid(:v_{});".format(column["column_name"],column["data_type"],column["column_name"]),file=f)
                    elif i > 0:
                         if column["data_type"] == 'USER-DEFINED':
                              print("   n_{} geometry := :v_{};".format(column["column_name"],column["column_name"]),file=f)
                         else:
                              print("   n_{} {} := :v_{};".format(column["column_name"],column["data_type"],column["column_name"]),file=f)
               
               #begin plsql - insert
               lines_2 =  ["\nBEGIN",
                           "INSERT INTO {}.{} ("
                         ]
               
               for line in lines_2:          
                    f.write(f"{line}\n".format(cortege["schemaname"], cortege["relname"]))
               
               for i, column in enumerate(cortege["columns"]):
                    
                    if i == 0:
                         print("   {}".format(column["column_name"]),file=f)
                    elif i > 0: print("   ,{}".format(column["column_name"]),file=f)  
               
               #begin plsql - values
               f.write(f")\nvalues (\n")
               
               for i, column in enumerate(cortege["columns"]):
                    
                    if i == 0:
                         print("   n_{}".format(column["column_name"]),file=f)
                    elif i > 0: print("   ,n_{}".format(column["column_name"]),file=f)              
               
               #end block
               f.write(f"\n   );")
               f.write(f"\nEND;")
               f.write(f"\n\">")
               f.write(f"\n</sql>\n")
     f.close()

def print_tree_upd(tree_,path_output_,file_):
    
     """
     Prints the tree created by get_tree
     """
     file_=path_output_+file_
     with open(file_, 'a', encoding="utf-8") as f:
          for cortege in tree_:
               lines_1 = ["-------------------------------",
                         "-- update BO {}",
                         "\n<sql clause = \"",
                         "DECLARE"]
               for line in lines_1:          
                    f.write(f"{line}\n".format(cortege["relname"]))     
               # assign var
               for i, column in enumerate(cortege["columns"]):
                    if i == 0:
                         primary_key = column["column_name"] # sign global primary_key for current cortege
                         print("   n_{} {} := uuid(:v_{});".format(column["column_name"],column["data_type"],column["column_name"]),file=f)
                    elif i > 0:
                         if column["data_type"] == 'USER-DEFINED':
                              print("   n_{} geometry := :v_{};".format(column["column_name"],column["column_name"]),file=f)
                         else: 
                              print("   n_{} {} := :v_{};".format(column["column_name"],column["data_type"],column["column_name"]),file=f)
               # assign old var
               for i, column in enumerate(cortege["columns"]):
                    if i == 0:
                         f.write(f"\n")
                    elif i > 0:
                         if column["data_type"] == 'USER-DEFINED':
                              print("   old_{} geometry;".format(column["column_name"]),file=f)
                         else:
                              print("   old_{} {};".format(column["column_name"],column["data_type"]),file=f)
                              
               #begin block plsql
               lines_2 =  ["\nBEGIN",
                           "SELECT"
                         ]
               for line in lines_2:          
                    f.write(f"{line}\n") 
               #fill old_ var
               for i, column in enumerate(cortege["columns"]):
                    if i == 0:
                         f.write(f"\n")
                    if i == len(cortege["columns"])-1:
                         print("   {}.{}".format(cortege["relname"],column["column_name"]),file=f)
                    elif i > 0:
                         print("   {}.{},".format(cortege["relname"],column["column_name"]),file=f)
                    
               f.write(f"\nINTO")
               
               for i, column in enumerate(cortege["columns"]):
                    if i == 0:
                         f.write(f"\n")
                    if i == len(cortege["columns"])-1:
                         print("   old_{}".format(column["column_name"]),file=f)
                    elif i > 0:
                         print("   old_{},".format(column["column_name"]),file=f)
               
               lines_3 =  ["\nFROM {}.{} as {}"]
               for line in lines_3:          
                    f.write(f"{line}\n".format(cortege["schemaname"],cortege["relname"],cortege["relname"]))            
               lines_3_1 = ["WHERE {}.{} = n_{};"]     
               for line in lines_3_1:          
                    f.write(f"{line}\n".format(cortege["relname"],primary_key,primary_key))                 
               
               f.write(f"\nIF")
               f.write(f"\n   MD5(")
               f.write(f"\n   ROW(")
               for i, column in enumerate(cortege["columns"]):
                    if i == 0:
                         f.write(f"\n")
                    if i == len(cortege["columns"])-1:
                         print("   old_{}".format(column["column_name"]),file=f)
                    elif i > 0:
                         print("   old_{},".format(column["column_name"]),file=f)
                         
               f.write(f"\n   )::text) !=")
               f.write(f"\n   MD5(")
               f.write(f"\n   ROW(")
               
               for i, column in enumerate(cortege["columns"]):
                    if i == 0:
                         f.write(f"\n")
                    if i == len(cortege["columns"])-1:
                         print("   n_{}".format(column["column_name"]),file=f)
                    elif i > 0:
                         print("   n_{},".format(column["column_name"]),file=f)
               f.write(f"\n   )::text)")
               # end block - set
               f.write(f"\nTHEN")
               lines_4 =  ["\n     UPDATE {}.{}",
                           "\n     SET"]
               for line in lines_4:          
                    f.write(f"{line}\n".format(cortege["schemaname"], cortege["relname"]))
                             
               for i, column in enumerate(cortege["columns"]):
                    if i == 0:
                         f.write(f"\n")
                    if i == len(cortege["columns"])-1:
                         print("          {} = n_{}".format(column["column_name"],column["column_name"]),file=f)
                    elif i > 0:
                         print("          {} = n_{},".format(column["column_name"],column["column_name"]),file=f)            
                    
               
               lines_5 =  ["\n          WHERE {} = n_{};"]
               for line in lines_5:          
                    f.write(f"          {line}".format(primary_key,primary_key)) 
               # end
               f.write(f"\nEND IF;")
               f.write(f"\nEND;")
               f.write(f"\n\">")   
               f.write(f"\n</sql>\n")
     f.close()

def print_tree_slct(tree_,path_output_,file_):
    
     """
     Prints the tree created by get_tree
     """
     file_=path_output_+file_
     with open(file_, 'a', encoding="utf-8") as f:
          for cortege in tree_:
               lines_1 = ["-------------------------------",
                         "-- select BO {}",
                         "SELECT"]
               for line in lines_1:          
                    f.write(f"{line}\n".format(cortege["relname"]))     
               # select block
               for i, column in enumerate(cortege["columns"]):
                    if i == 0:
                         print("   {}.{} as v_{}".format(cortege["relname"],column["column_name"],column["column_name"]),file=f)
                    else:
                         print("   ,{}.{} as v_{}".format(cortege["relname"],column["column_name"],column["column_name"]),file=f)             
               print("\nFROM {}.{} as {}".format(cortege["schemaname"],cortege["relname"],cortege["relname"]),file=f)
     f.close()

def print_tree_var(tree_,path_output_,file_):
    
     """
     Prints the tree created by get_tree
     """
     file_=path_output_+file_
     with open(file_, 'a', encoding="utf-8") as f:
          for cortege in tree_:  
               # var transfer
               lines_1 = ["-------------------------------",
                         "-- sign var BO {}"]
               for line in lines_1:          
                    f.write(f"{line}\n".format(cortege["relname"]))   
               for i, column in enumerate(cortege["columns"]):
                    print("<attribute name=\"v_{}\" />".format(column["column_name"]),file=f)             
     f.close()

