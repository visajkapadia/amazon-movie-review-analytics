import base64
import os
import logging
from google.cloud import storage

def move_success_file(event, context):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    filename = pubsub_message

    source_bucket_id = os.environ.get("SOURCE_BUCKET")
    blob_name = filename
    destination_bucket_id = os.environ.get("DESTINATION_BUCKET")
    destination_folder = os.environ.get("DESTINATION_FOLDER")
    new_blob_name = destination_folder + "/" + filename
    logging.info("Moving file: {}".format(filename))

    storage_client = storage.Client()

    source_bucket = storage_client.get_bucket(source_bucket_id)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.get_bucket(destination_bucket_id)

    new_blob = source_bucket.copy_blob(source_blob, destination_bucket, new_blob_name)

    source_blob.delete()
