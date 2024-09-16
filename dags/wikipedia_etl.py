from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook

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

# Defining DAG
@dag(
    dag_id='dag_populations_wikipedia',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['population', 'wikipedia']
)
def population_wikipedia_dag():

    @task
    def extract_data():
        """
        Task to extract data from the Wikipedia page and save it to a CSV file.
        """
        url = "https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population"
        output_path = 'populations_wikipedia.csv'
        return extract_from_wikipedia_url(url, output_path)
    
    @task
    def upload_to_gcs(file_path: str):
        """
        Task to upload the extracted CSV file to a Google Cloud Storage bucket.
        """
        bucket_name = 'wikipedia_population'
        destination_path = 'raw/populations_wikipedia.csv'
        
        # Use GCSHook to upload the file
        gcs_hook = GCSHook(gcp_conn_id="gcp")
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=destination_path,
            filename=file_path,
            mime_type='text/csv'
        )

    # Task dependencies
    file_path = extract_data()
    upload_to_gcs(file_path)

# Instantiate the DAG
dag_instance = population_wikipedia_dag()
