import csv
from google.cloud import storage
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.api_core.exceptions import NotFound
import win32com.client as win32
from google.cloud import bigquery
from datetime import datetime
from datetime import date
from pandas import DataFrame
import time
import os

def send_email(subject, body, to_address):
    '''
    Sends an email via Outlook.
    Parameters:
        subject (str): The email subject.
        body (str): The email body.
        to_address (str): The recipient's email address.
    '''
    olApp = win32.Dispatch('Outlook.Application')
    olNS = olApp.GetNamespace('MAPI')
    mailItem = olApp.CreateItem(0)
    mailItem.Subject = subject
    mailItem.BodyFormat = 1
    mailItem.Body = body
    mailItem.To = to_address
    mailItem.Sensitivity = 2
    mailItem.Send()
  
def update_table_descriptions(dataset_id):
    """
    Updates the table descriptions in BigQuery for a given dataset based on mappings provided. Essentially, adds a title summary/description of the entire table in BigQuery. 
    
    :param dataset_id: The ID of the dataset in BigQuery where tables are located.
    :return: None
    """
    # Initialize a BigQuery client
    client = bigquery.Client(project=project)

    # Loop through the mappings and update the table descriptions
    for mapping in file_mappings:
        table_name = mapping.get('table_name')
        detail_description = mapping.get('detail_description')  # Use .get() to safely get the value

        if not table_name or not detail_description:
            print("Table name or detail description is missing in the mapping.")
            continue

        # Get the reference to the table
        table_ref = client.dataset(dataset_id).table(table_name)

        try:
            # Fetch the table metadata
            table = client.get_table(table_ref)
            
            # Update the description
            table.description = detail_description
            
            # Update the table with new metadata
            client.update_table(table, ["description"])
            print(f"Updated description for table {table_name} in dataset {dataset_id}.")
        except bigquery.NotFound:
            print(f"Table {table_name} not found in dataset {dataset_id}.")

def send_to_bucket(df, bucket_name, destination_blob_name):
    """
    Uploads a DataFrame to Google Cloud Storage (GCS) with a specific destination path, performing a full replace
    of existing files with the same name.

    Args:
        df (pd.DataFrame): The DataFrame to be uploaded.
        bucket_name (str): The name of the GCS bucket.
        destination_blob_name (str): The name of the destination blob in GCS.

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    # Convert DataFrame to CSV content in memory
    csv_content = df.to_csv(index=False).encode()

    try:
        # Create a GCS client
        storage_client = storage.Client()

        # Get the GCS bucket
        bucket = storage_client.bucket(bucket_name)

        # Upload the CSV content to the GCS bucket, performing a full replace of existing files
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(csv_content, content_type="text/csv", if_generation_match=None)

        print(f"{destination_blob_name.split('/')[-1]} uploaded to destination bucket successfully")
        return True

    except Exception as e:
        # Handle the exception and indicate failure
        print(f"Failed to upload {destination_blob_name.split('/')[-1]} to GCS bucket: {str(e)}")
        return False

def send_data_to_bigquery(project_id, dataset_id, gcs_uri, table_name, schema_description=None):
    """
    Sends data from a Google Cloud Storage (GCS) file to a BigQuery table with the specified or auto-detected schema.

    Parameters:
        project_id (str): The ID of the Google Cloud project.
        dataset_id (str): The ID of the BigQuery dataset where the table should be created or updated.
        gcs_uri (str): The URI of the GCS file containing the data to be loaded into BigQuery.
        table_name (str): The name of the BigQuery table where the data will be loaded.
        schema_description (list, optional): A list of dictionaries specifying the schema of the target BigQuery table.
                                             Each dictionary should contain 'name', 'type', and 'description' keys.
                                             If None, the schema will be auto-detected.

    Returns:
        None
    """

    # Create a BigQuery client
    client = bigquery.Client(project=project_id)

    # Create the table reference with the table name
    table_ref = client.dataset(dataset_id).table(table_name)

    # Create a job configuration
    job_config = bigquery.LoadJobConfig(
        schema=schema_description if schema_description else None,  # Set the schema if provided, else None
        source_format=bigquery.SourceFormat.CSV,  # Change this based on your data format
        skip_leading_rows=1,  # Skip header row if applicable
        autodetect=True if schema_description is None else False,  # Auto-detect schema if none provided
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Truncate the table before loading
    )

    # Start the job to load data from GCS to BigQuery
    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    # Check if the job was successful
    if load_job.state == "DONE":
        print(f"{table_name} successfully loaded from GCS bucket to BigQuery")
    else:
        print(f"Error loading data from GCS to BigQuery for table {table_name}.")

def update_bigquery_table_schemas(file_mappings, project, target_dataset, variable_name_col, description_col, data_type_col=None):
    """
    Updates BigQuery table schemas based on available descriptions (and optionally data types) from a CSV, Excel, or DataFrame.
    Prints out columns for which a description could not be found.

    :param file_mappings: List of mappings with table names and schema file or function returning a DataFrame
    :param project: Google Cloud project ID
    :param target_dataset: BigQuery dataset name
    :param variable_name_col: Column name for BigQuery column names
    :param description_col: Column name for descriptions
    :param data_type_col: Optional column name for data types
    """
    client = bigquery.Client(project=project)

    for file_mapping in file_mappings:
        table_name = file_mapping['table_name']
        schema_source = file_mapping['schema_file']
        file_type = file_mapping.get('file_type', 'dataframe')

        # Reading the schema source
        if callable(schema_source):
            schema_df = schema_source()
        elif isinstance(schema_source, DataFrame):
            schema_df = schema_source
        elif file_type == 'excel':
            schema_df = pd.read_excel(schema_source)
        elif file_type == 'csv':
            schema_df = pd.read_csv(schema_source)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        # Mapping and updating schemas
        schema_mapping = dict(zip(schema_df[variable_name_col], schema_df[description_col]))
        table_ref = client.dataset(target_dataset).table(table_name)
        table = client.get_table(table_ref)

        missing_descriptions = []
        new_schema = []
        for field in table.schema:
            column_name = field.name
            description = schema_mapping.get(column_name, "")
            if not description:
                missing_descriptions.append(column_name)
            new_field = bigquery.SchemaField(column_name, field.field_type, description=description, mode=field.mode)
            new_schema.append(new_field)

        table.schema = new_schema
        client.update_table(table, ["schema"])

        print(f"Schema updated for table {table_name}.")
        if missing_descriptions:
            print(f"Columns missing descriptions in {table_name}: {', '.join(missing_descriptions)}")
        else:
            print(f"All columns in {table_name} have descriptions.")

    print(f"All schemas updated and sent to BigQuery dataset {target_dataset}.")

def get_df_from_bigquery(project, dataset, table_name):
    """
    Fetches data from a specified table in Google BigQuery and returns it as a pandas DataFrame.

    :param project: The Google Cloud project ID.
    :param dataset: The BigQuery dataset name.
    :param table_name: The name of the table from which to fetch the data.
    :return: A pandas DataFrame containing the data from the specified BigQuery table.
    """
    # Initialize a BigQuery client with the specified project
    client = bigquery.Client(project=project)

    # Construct the full table path
    table_path = f"`{project}.{dataset}.{table_name}`"

    # Define the SQL query to fetch all data from the table
    query = f"SELECT * FROM {table_path}"

    # Execute the query and return the result as a pandas DataFrame
    return client.query(query).to_dataframe()