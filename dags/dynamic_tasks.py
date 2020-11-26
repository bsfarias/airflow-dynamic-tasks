import datetime as dt
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

local_tz = pendulum.timezone("America/Sao_Paulo")
args = {
    'owner': 'me',
    'start_date': dt.datetime(2020, 11, 10,  tzinfo=local_tz),
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=5)
}

with DAG('dynamic_tasks', default_args=args, catchup=False, schedule_interval=None) as dag:
    #task dummy para inicializar o pipeline
    begin = DummyOperator(
            task_id='begin'
    )

    #task dummy para finalizar o pipeline
    end = DummyOperator(
          task_id='end'
    )    

    #tasks geradas dinamicamente
    for i in range(1,11):
        dynamic_task = DummyOperator(
                       task_id=f'dynamic_task{i}'
        )
        begin >> dynamic_task >> end
    

