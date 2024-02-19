from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from data_clean_function import pokem_report
from datetime import datetime, timedelta


default_args = {
    'owner': 'obaliuta',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'schedule_interval': timedelta(days=1),
    'file_to_read': 'pokedex.csv'
}


# dag entity
dag = DAG(
    dag_id='etl_001',
    default_args=default_args
)

# sensor -- waits for a file to proceed
sensor = FileSensor(task_id='sense_file', 
                    filepath='/home/repl/workspace/dag_1_trigger.txt',
                    poke_interval=45, #3 checks for a file each 45 sec
                    dag=dag)

bash_task = BashOperator(task_id='cleanup_tempfiles', 
                         bash_command='rm -f /mnt/c/Users/NoteBook/Desktop/Pet Projects/Git Hub/airflow_dags/*.tmp',
                         dag=dag)

python_task = PythonOperator(task_id='run_processing', 
                             python_callable=pokem_report,
                             op_kwargs={'file_to_read' : default_args.file_to_read},
                             provide_context=True,
                             dag=dag)

email_subject="""
  Hi {{ params.name }}. Here is the report from {{ ds_nodash }}
"""

email_report_task = EmailOperator(task_id='email_report_task',
                                  to='sales@mycompany.com',
                                  subject=email_subject,
                                  html_content='',
                                  params={'name': 'John'},
                                  dag=dag)

no_email_task = EmptyOperator(task_id='no_email_task', dag=dag)

def check_weekend(**kwargs):
    dt = datetime.strptime(kwargs['execution_date'],"%Y-%m-%d")
    # If dt.weekday() is 0-4, it's Monday - Friday. If 5 or 6, it's Sat / Sun.
    if (dt.weekday() < 5):
        return 'email_report_task'
    else:
        return 'no_email_task'
    
branch_task = BranchPythonOperator(task_id='check_if_weekend',
                                   python_callable=check_weekend,
                                   provide_context=True,
                                   dag=dag)

    
sensor >> bash_task >> python_task

python_task >> branch_task >> [email_report_task, no_email_task]






