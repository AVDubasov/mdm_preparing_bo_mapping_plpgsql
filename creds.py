import os

#Path env

os.chdir("usr_local_path")
path_parent=os.path.abspath('..')
path_output=path_parent+'\\output\\mdm\\'

#DataBase

server = 'localhost' 
table ='tb_foot_print'
name_db = 'name_bd'
user = 'bd_user'
pwd = 'bd_pwd'
prt = '5432'
crt = '~/.mysql/root.crt' # certificat for ssl connection to mysql

