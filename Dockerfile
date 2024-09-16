# Use the official Airflow image as a base
FROM apache/airflow:2.10.1

# Copy the requirements file to the container
COPY requirements.txt /requirements.txt

# Install dbt
RUN pip install dbt