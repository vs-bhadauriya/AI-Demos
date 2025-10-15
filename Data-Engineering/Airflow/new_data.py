"""
Airflow DAG to download a file from SFTP to local filesystem using a custom connection ID.

- Connection ID: sftp_custom
- Remote file: /data/upload/sample.txt
- Local file: /tmp/sample.txt
"""

from airflow import DAG
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.operators.python import PythonOperator
from datetime import datetime

# Airflow SFTP connection ID
FTP_CONN_ID = 'sftp_custom'

# Remote and local paths
REMOTE_PATH = '/data/upload/sample.txt'
LOCAL_PATH = '/mnt/shared/data/sample.txt'


def download_file_from_sftp():
    """
    Downloads a file from the SFTP server to local filesystem.
    """
    sftp = SFTPHook(ftp_conn_id=FTP_CONN_ID)
    sftp.get(remote_path=REMOTE_PATH, local_path=LOCAL_PATH)
    print(f"Downloaded {REMOTE_PATH} to {LOCAL_PATH}")


with DAG(
    dag_id='sftp_download_sample_file',
    start_date=datetime(2025, 10, 15),
    schedule_interval=None,
    catchup=False,
    tags=['sftp', 'download']
) as dag:

    download_task = PythonOperator(
        task_id='download_sample_txt',
        python_callable=download_file_from_sftp
    )
