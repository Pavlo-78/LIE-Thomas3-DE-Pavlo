import sqlite3
import os
# import sys
# sys.stdout.reconfigure(encoding='utf-8')
# raise SystemExit

def dbcon():
# set the pass
# print(os.getcwd()) #check the pass
    # print('CURRENT DIRECTORY FF2=1',os.getcwd())
    # os.chdir(r"/c/!Git/300_Airflow2/dags/tools")
    os.chdir(r"/mnt/c/!Git/300_Airflow2/dags/tools")
    print('CURRENT DIRECTORY FF2=1',os.getcwd())
    
    # Connecting to the SQLite database
    conn = sqlite3.connect(r"air.db")

    # # SQL query
    ss='SELECT * FROM wines limit 10'
    # # Вивід результату
    for row in conn.cursor().execute(ss): print(row)

    sss = r"insert into temp1 values(2,'test')"
    # conn.cursor().execute(sss)
    conn.execute(sss)
    conn.commit()

    # Closing the connection
    conn.close()
    return 'dbcon OK2'

dbcon()