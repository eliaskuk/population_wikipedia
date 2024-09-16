from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
import pandas as pd
import re

def extract_from_wikipedia_url(url, output_path):
    """
    Extracts data from a Wikipedia URL and saves it to a CSV file.

    :param url: URL of the Wikipedia page containing the table
    :param output_path: Path where the CSV file will be saved
    :return: Path to the saved CSV file
    """
    # Read all HTML tables from the URL
    tables = pd.read_html(url)

    # First (0) table is the population table
    population_table = tables[0]
    population_table.to_csv(output_path, index=False)

    return output_path

def clean_column_names(file_path):
    """
    Cleans column names in the CSV file to be compatible with BigQuery.

    :param file_path: Path to the CSV file to be cleaned
    :return: Path to the cleaned CSV file
    """
    df = pd.read_csv(file_path)
    
    # Clean column names
    df.columns = [re.sub(r'\W', '_', col) for col in df.columns]
    
    # Save the cleaned CSV
    cleaned_file_path = file_path.replace('.csv', '_cleaned.csv')
    df.to_csv(cleaned_file_path, index=False)
    
    return cleaned_file_path

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='dag_populations_wikipedia',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['population', 'wikipedia'],
) as dag:

    # Task to extract data from Wikipedia
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_from_wikipedia_url,
        op_args=[
            "https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population",
            '/tmp/populations_wikipedia.csv'
        ]
    )

    # Task to clean column names
    clean_headers = PythonOperator(
        task_id='clean_headers',
        python_callable=clean_column_names,
        op_args=['/tmp/populations_wikipedia.csv']
    )

    # Task to upload cleaned CSV to GCS
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src='/tmp/populations_wikipedia_cleaned.csv',
        dst='raw/populations_wikipedia_cleaned.csv',
        bucket='wikipedia_population', 
        gcp_conn_id='gcp',
        mime_type='text/csv'
    )

    # Task to load data into BigQuery
    load_data_to_bigquery = BigQueryInsertJobOperator(
        task_id='load_data_to_bigquery',
        configuration={
            "load": {
                "sourceUris": ["gs://wikipedia_population/raw/populations_wikipedia_cleaned.csv"],
                "destinationTable": {
                    "projectId": "airflow-project-435112",
                    "datasetId": "populations",
                    "tableId": "wikipedia_population_raw_data"
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,  # Skip the header row
                "autodetect": True,  # Automatically detect schema
                "schemaUpdateOptions": ["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
                "charMapV2": True  # Enable Character Map V2
            }
        },
        gcp_conn_id='gcp'
    )

    # Set task dependencies
    extract_data >> clean_headers >> upload_to_gcs >> load_data_to_bigquery
