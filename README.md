## Population Data Pipeline with Airflow and Google Cloud
This project demonstrates an Apache Airflow pipeline for extracting population data from Wikipedia, cleaning it, and loading it into Google BigQuery. The project is designed to run in a Docker environment, making it easy to deploy and scale.

### Project Structure
dags/: Contains the Airflow DAG that extracts and processes population data.

Dockerfile: Defines the Airflow environment.

docker-compose.yaml: Configures the Airflow services for local execution.

populations_wikipedia.csv: Sample CSV file for testing the pipeline.

requirements.txt: Lists Python dependencies for Airflow and Google Cloud providers.

### Pipeline Overview
Extract Data: Extracts population data from Wikipedia.

Clean Data: Cleans column names to ensure compatibility with BigQuery.

Upload to GCS: Uploads the cleaned data to a Google Cloud Storage bucket.

Load into BigQuery: Loads the data from GCS into a BigQuery table.
