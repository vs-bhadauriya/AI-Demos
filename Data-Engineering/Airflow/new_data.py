"""
Airflow DAG to download a file from SFTP to local filesystem using a custom connection ID.

- Connection ID: sftp_custom
- Remote file: /data/upload/sample.txt
- Local file: /mnt/shared/data/sample.txt
"""

from airflow import DAG
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# Airflow SFTP connection ID
FTP_CONN_ID = 'sftp_custom'

# Remote and local paths
REMOTE_PATH = '/data/upload/sample.txt'
LOCAL_PATH = '/mnt/shared/data/sample.txt'


def download_file_from_sftp():
    """
    Download a file from SFTP server using Paramiko SFTPClient.
    Compatible with Airflow 2.5+.
    """
    # Ensure local directory exists
    os.makedirs(os.path.dirname(LOCAL_PATH), exist_ok=True)

    hook = SFTPHook(ftp_conn_id=FTP_CONN_ID)
    sftp_client = hook.get_conn()  # returns paramiko.SFTPClient

    try:
        sftp_client.get(REMOTE_PATH, LOCAL_PATH)
        print(f"Downloaded {REMOTE_PATH} to {LOCAL_PATH}")
    except Exception as e:
        print(f"Error downloading file: {e}")
        raise
    finally:
        sftp_client.close()


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
