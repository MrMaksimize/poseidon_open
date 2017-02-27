"""inventory_dags file."""
from airflow.operators.python_operator import PythonOperator
from poseidon.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.models import DAG
from poseidon.util import general
from poseidon.util.notifications import notify
from poseidon.dags.inventory.inv_jobs import *


conf = general.config

args = general.args

schedule = general.schedule['inventory']

dag = DAG(dag_id='inventory', default_args=args, schedule_interval=schedule)


#: Inventory Doc To CSV
inventory_to_csv = PythonOperator(
    task_id='inventory_to_csv',
    python_callable=inventory_to_csv,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload Inventory CSV to S3
upload_inventory = S3FileTransferOperator(
    task_id='upload_inventory',
    source_base_path=conf['prod_data_dir'],
    source_key='inventory_datasd.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='inventory/inventory_datasd.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)



#: Execution Rules
#: Inventory csv gets created before its uploaded
upload_inventory.set_upstream(inventory_to_csv)
