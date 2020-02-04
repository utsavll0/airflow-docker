import airflow
from airflow import DAG
from airflow.operators.papermill_operator import PapermillOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'Utsav',
    'start_date': datetime(2019,1,25),
}

dag = DAG('papermill_DAG', default_args=default_args, schedule_interval=None)


t1=PapermillOperator(
    task_id="Job_Schedular",
    input_nb="schedular.ipynb",
    #output_nb="op-{{execution_date}}.ipynb",
    output_nb="op1.ipynb",
    parameters={"msgs": "Ran from Airflow at {{ execution_date }}!"},
    dag=dag,
)

t2=BashOperator(
    task_id="Finished",
    bash_command="echo Finished",
    dag=dag,
)
t1.set_downstream(t2)