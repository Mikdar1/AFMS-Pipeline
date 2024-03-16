from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import csv
from tempfile import NamedTemporaryFile
from supabase import create_client, Client
url = 'https://zdrgurrkwghrbtzfttmf.supabase.co'
key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inpkcmd1cnJrd2docmJ0emZ0dG1mIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDc1NzI1NzYsImV4cCI6MjAyMzE0ODU3Nn0.g8HNHTMNXp_2qiTqbe4_GtzyYZuNd-96X8MqMK5QUSM'
supabase = create_client(url, key)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'supabase_data_export',
    default_args=default_args,
    description='Export Supabase data to CSV and upload to storage',
    schedule_interval='@daily',  
)


    


def export_supabase_data(idlahan, **kwargs):
    response_storage = supabase.table('dataset').select("id_lahan").order('id_lahan', desc=True).limit(1).execute()
    response_storage_num = response_storage.data[0].get('id_lahan')

    for i in range(response_storage_num):
        response_table = supabase.table('dataset').select("moisture_before, weather_code, moisture_after").order('input_sequence', desc=True).eq("id_lahan", idlahan).limit(1000).execute()
        
        csv_filename = f'moisture_before_after_{idlahan}.csv'
        
        with NamedTemporaryFile(mode='w', newline='', delete=False) as csv_file:
            fieldnames = ["moisture_before", "weather_code", "moisture_after"]
            data_to_write = response_table.data
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

            writer.writeheader()

            for row in data_to_write:
                writer.writerow(row)

            csv_file_name = csv_file.name  # Get the temporary file name
            supabase.storage.from_('ml-data-feeds').remove(csv_filename)
        with open(csv_file_name, 'rb') as upload:
            supabase.storage.from_('ml-data-feeds').upload(csv_filename, csv_file_name, file_options={"content-type": "text/csv"})
        
        idlahan=idlahan+1



export_data_task = PythonOperator(
    task_id='export_supabase_data',
    python_callable=export_supabase_data,
    provide_context=True,
    op_args=[1],  
    dag=dag,
)
export_data_task
if __name__ == "__main__":
    dag.cli()
