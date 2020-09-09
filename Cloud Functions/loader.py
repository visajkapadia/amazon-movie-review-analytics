from google.cloud import bigquery
from google.cloud import pubsub_v1
import os
import logging


def load_data_to_table(event, context):
     project_id = os.environ.get("PROJECT_ID")
     success_topic_id = os.environ.get("SUCCESS_TOPIC_ID")
     error_topic_id = os.environ.get("ERROR_TOPIC_ID")
     source_bucket = os.environ.get("SOURCE_BUCKET")
     table_id = os.environ.get("TABLE_ID") 
     client = bigquery.Client()
     filename = event['name']
     logging.info("Loading file: {}".format(filename))

     job_config = bigquery.LoadJobConfig(
     schema=[
          bigquery.SchemaField("sentiment", "INTEGER"),
          bigquery.SchemaField("text", "STRING"),
          bigquery.SchemaField("summary", "STRING"),
          bigquery.SchemaField("time", "INTEGER"),
          bigquery.SchemaField("score", "FLOAT"),
          bigquery.SchemaField("movie_id", "STRING"),
          bigquery.SchemaField("helpfulness", "FLOAT"),
          bigquery.SchemaField("user_name", "STRING"),
          bigquery.SchemaField("user_id", "STRING"),
     ],
     source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
     )
     uri = "gs://" + source_bucket + "/" + filename

     load_job = client.load_table_from_uri(
     uri,
     table_id,
     location="US",  
     job_config=job_config,
     )  

     res = load_job.result() 
     print(res.error_result)
     print(res.errors)

     destination_table = client.get_table(table_id)
     logging.info("Loaded {} rows.".format(destination_table.num_rows))

     has_error = False
     topic_id = error_topic_id if has_error else success_topic_id

     data = str.encode(filename)
     publisher = pubsub_v1.PublisherClient()
     topic_path = publisher.topic_path(project_id, topic_id)
     future = publisher.publish(topic_path, data)
     logging.info("Publishing message to topic id: {}".format(topic_id))
     logging.info(future.result())
