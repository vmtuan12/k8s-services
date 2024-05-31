from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

# Define the DAG
sshHook = SSHHook(ssh_conn_id="ssh-196")

dag = DAG(
    'ssh-dag',
    description='ssh dag',
    schedule_interval=None,
    start_date=datetime(2024, 3, 25),
    catchup=False
)

# Define the BashOperator task
ls = SSHOperator(
    task_id="ls",
    command= "ls -l /home/",
    ssh_hook = sshHook,
    dag = dag
)

ls