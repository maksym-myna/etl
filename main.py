from time import sleep
from uuid import uuid4
from bigquery_schemas import schemas
import db_secrets as secrets
import queries
from datetime import datetime
from itertools import groupby
import logging

import os

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io import BigQueryDisposition
import apache_beam as beam

from sqlalchemy import text
import sqlalchemy

from google.cloud.sql.connector import Connector
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from google.cloud import bigquery, storage


class ReadFromPostgres(beam.DoFn):
    def __init__(self, credentials):
        self.__processed = []
        self.credentials = credentials
        self.secrets = secrets

    def process(self, element, table_name: str):
        if element in self.__processed:
            return
        self.__processed.append(element)
        INSTANCE_CONNECTION_NAME = (
            f"{self.secrets.PROJECT_ID}:{self.secrets.REGION}:{self.secrets.INSTANCE_ID}"
        )
        sql_connector = Connector(credentials=self.credentials)
        conn = sql_connector.connect(
            INSTANCE_CONNECTION_NAME,
            "pg8000",
            user=secrets.DB_USER,
            password=secrets.DB_PASS,
            db=secrets.DB_NAME,
        )

        pool = sqlalchemy.create_engine(
            "postgresql+pg8000://", creator=lambda: conn, pool_timeout=36000
        )
        print(f'Procesing data from {table_name}', flush=True)
        with pool.connect() as db_conn:
            with db_conn.begin():
                # print(element, flush=True)  # Print the query
                result = db_conn.execute(text(element))
                rows = [row._asdict() for row in result]
        return rows


class DataPipeline:
    def __init__(self):
        self.pipeline_options = PipelineOptions(
            [
                f"--project={secrets.PROJECT_ID}",
                "--runner=DirectRunner",
                f"--temp_location=gs://{secrets.BUCKET_NAME}/temp",
                f"--region={secrets.REGION}",
                "--setup_file=./setup.py"
            ]
        )
        CREDENTIALS = r"M:\Personal\SE\Term 6\Data Warehouses\data population\scripts\cloudsql\credentials.json"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS
        
        self.credentials = service_account.Credentials.from_service_account_file(CREDENTIALS)
        # self.credentials = default()
        
        self.bigquery_client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)
        self.storage_client = storage.Client(credentials=self.credentials, project=self.credentials.project_id)
        
        self.bucket = self.storage_client.bucket(secrets.BUCKET_NAME)
        
        self.p = beam.Pipeline(options=self.pipeline_options)
        self.read_from_postgres = ReadFromPostgres(self.credentials)



    def generate_merge_sql(self, table_name, schema):
        try:
            print(f"Merging {table_name}s...", flush=True)
            # Generate the SET clause for the UPDATE statement
            set_clause = ", ".join(
                [f"TARGET.{field.name} = SOURCE.{field.name}" for field in schema]
            )

            # Generate the fields and values for the INSERT statement
            insert_fields = ", ".join([field.name for field in schema])
            insert_values = ", ".join([f"SOURCE.{field.name}" for field in schema])

            pk_fields = [field for field in schema if field.description and "PK" in field.description]
            on_clause = " AND ".join(
                [f"TARGET.{field.name} = SOURCE.{field.name}" for field in pk_fields]
            )

            merge_sql = f"""
            MERGE `{secrets.PROJECT_ID}.{secrets.INSTANCE_ID}.{table_name}` AS TARGET
            USING `{secrets.PROJECT_ID}.{secrets.INSTANCE_ID}_staging.{table_name}` AS SOURCE
            ON {on_clause}
            WHEN MATCHED THEN
                UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_fields}) 
                VALUES ({insert_values})
            """
            # print(merge_sql, flush=True)
            return self.bigquery_client.query(merge_sql)
        except Exception as e:
            print(table_name, e, flush=True)



    def get_query(self, start_date, key="", get_all=False):
        qs = {
            "work": queries.work.format(start_date=start_date),
            "user": queries.user.format(start_date=start_date),
            "publisher": queries.publisher.format(start_date=start_date),
            "author": queries.author.format(start_date=start_date),
            "work_author": queries.work_author.format(start_date=start_date),
            "subject": queries.subject.format(start_date=start_date),
            "language": queries.language.format(start_date=start_date),
            "date": queries.date.format(start_date=start_date),
            "medium": queries.medium,
            "listing_type": queries.listing_type,
            "rating_fact": queries.rating_fact.format(start_date=start_date),
            "listing_fact": queries.listing_fact.format(start_date=start_date),
            "return_fact": queries.return_fact.format(start_date=start_date),
        }
        return qs if get_all else qs.get(key, None)

    def get_or_create_table(self, table_name, staging = False):
        table_id = f"{secrets.PROJECT_ID}.{secrets.INSTANCE_ID}{'_staging' if staging else ''}.{table_name}"
        try:
            table = self.bigquery_client.get_table(table_id)
        except NotFound:
            table = bigquery.Table(table_id, schema=schemas[table_name])
            table = self.bigquery_client.create_table(table)
        return table_id

    def set_keys(self):
        for key, value in schemas.items():
            table_name = key
            fk_fields = ([(field.name, field.description.split()[-1]) for field in value if field.description and "FK" in field.description])
            [self.bigquery_client.query(f"ALTER TABLE library.{table_name} ADD constraint FK_{table_name}_{str(uuid4())} foreign key ({column_name}) references library.{references}({column_name}) not enforced") for column_name, references in fk_fields] 
            pk_fields = [(table_name, field.name) for field in value if field.description and "PK" in field.description]
            pk_fields.sort(key=lambda x: x[0])
            pk_fields_joined = {table: ', '.join(field for _, field in group) for table, group in groupby(pk_fields, key=lambda x: x[0])}
            [self.bigquery_client.query(f"ALTER TABLE library.{table_name} ADD primary key ({field}) not enforced") for field in pk_fields_joined.values()]
            

    def create_pipeline(self, table_name: str, start_date: str):
        print(f"Creating tables for {table_name}", flush=True)
        
        self.get_or_create_table(table_name)
        table_id = self.get_or_create_table(table_name, staging=True)

        (
            self.p
            | f"Creating query for {table_name}" 
            >> beam.Create([self.get_query(start_date=start_date, key=table_name)])
            | f"Reading {table_name} from Cloud SQL"
            >> beam.ParDo(self.read_from_postgres, table_name)
            | f"Writing {table_name} to the temporary BigQuery table"
            >> WriteToBigQuery(
                table_id,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                method="STREAMING_INSERTS",
            )
        )


    def run_pipeline(self):
        try:
            blob = storage.Blob("last_run.txt", self.bucket)
            start_date = blob.download_as_text().strip()
        except (NotFound, ValueError):
            start_date = "1900-01-01"
            
        self.bigquery_client.create_dataset(dataset=secrets.INSTANCE_ID, exists_ok=True)
        self.bigquery_client.create_dataset(dataset=f"{secrets.INSTANCE_ID}_staging", exists_ok=True)

        queries = self.get_query(start_date, get_all=True).keys()
        # for query in queries:
        #     table_id = f"{secrets.PROJECT_ID}.{secrets.INSTANCE_ID}_staging.{query}_temp"
        #     print(f"truncating {table_id}", flush=True)
        #     client.query(f"DELETE FROM {table_id} WHERE TRUE").result()

        ran_at = datetime.now()
        print(start_date)
        # start_date = "1900-01-01"

        for query in queries:
            self.create_pipeline(query, start_date)

        print("Running pipeline", flush=True)
        result = self.p.run()
        result.wait_until_finish()

        futures = []
        for query in queries:
            futures.append(self.generate_merge_sql(query, schemas[query]))

        for future in futures:
            future.result()

        blob = storage.Blob("last_run.txt", self.bucket)
        blob.upload_from_string(str(ran_at))
        
        self.set_keys()
        
        self.bigquery_client.delete_dataset(f"{secrets.INSTANCE_ID}_staging", delete_contents=True, not_found_ok=True) 
        self.bigquery_client.create_dataset(dataset=f"{secrets.INSTANCE_ID}_staging", exists_ok=True)
        for query in queries:
            self.get_or_create_table(query, staging=True)

    def main(self):
        self.run_pipeline()

if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.main()
