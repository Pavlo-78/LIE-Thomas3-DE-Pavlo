from datetime import datetime, timedelta
from airflow import DAG
# from airflow import Dataset
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import time
import sys
# sys.path.append('/mnt/c/!Git/300_Airflow2/')
# from tools import FF

import os
# os.chdir(r"/mnt/c/!beCodeGit/12_LIE-Thomas3-DE-Pavlo")
# from ..include import FF2
# from /mnt/c/beCodeGit/12_LIE-Thomas3-DE-Pavlo/include import FF2
# import main

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent)) # Add the parent directory to sys.path
# from include import FF2
from include import scrap2

# Instantiate a DAG with the specified parameters
dag1 = DAG(
    'a_scrapling2',
    default_args={  'owner':       'airflow',
                    'retries':      5,
                    'retry_delay':  timedelta(minutes=15),    
                    'start_date':   datetime(2023, 12, 10, 2, 30, 0),  #date from
                    },
                    # 'start_date': datetime(2023, 1, 1),
                    # 'start_date': datetime(2023, 12, 1, 22, 0, 0),  #date from
    # The interval 
    schedule_interval=timedelta(days=340),        # Run every day
    # schedule_interval=timedelta(hours=1),       # Run every hour
    # schedule_interval=timedelta(minutes=0.1),   # Run every 6 sec
    # schedule_interval=timedelta(minutes=2),     # Run every minutes
    # schedule_interval=timedelta(seconds=60),    # Run every .....
     # Set to False to ignore any historical runs
    catchup=False, 
    # dag_run_timeout=timedelta(minutes=60),
    #log_level='DEBUG',
)



# Python function to be executed
def execute_task(x, **kwargs):
    # messages
    print(f"Executing task: {x}")
    print('CURRENT DIRECTORY DAG=1',os.getcwd())
  # task logic -------------------------------------------------------------------  
    max_workers = 50 
    print(f"a = scrap2.start_scrap(pages=10, max_workers={max_workers}, fpath='.')")
    a = scrap2.start_scrap(pages=1, max_workers=max_workers, fpath='.')
    print(a)


# Create the initial dummy task to start the DAG
t1 = DummyOperator(task_id='task_t1',  dag = dag1, )

t2 = PythonOperator(
        task_id = 'scrap_immoweb',
        python_callable=execute_task,
        op_args = [1],
        provide_context = True,
        # -time-out to stop task by airflow
        execution_timeout = timedelta( hours = 5 ), # Time limit for the execution
        # timeout=60, 
        dag = dag1,  
        )

te = DummyOperator(task_id='task_te', dag=dag1,)


# Set task dependencies
t1 >> t2 >> te



# FF.ttf(2)