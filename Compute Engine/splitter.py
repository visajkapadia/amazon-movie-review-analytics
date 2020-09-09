from google.cloud import storage
from googleapiclient import discovery
import os
import logging
import uuid
import pandas as pd
import numpy as np
import requests
from io import StringIO
source_bucket_id = "egen-original-dataset"
project_name = 'egen-final-project'
destination_bucket_id = "input-text-data"
instance = 'file-splitter'
zone = 'us-central1-a'
client = storage.Client()
source_bucket = client.get_bucket(source_bucket_id)
service = discovery.build('compute', 'v1', cache_discovery=False)
request = service.instances().get(project=project_name, zone=zone, instance=instance)
response = request.execute()
metadata_keyvalue = response["metadata"]["items"]
source_bucket_filename = metadata_keyvalue[0]["value"]
total_records = 30000
filename = source_bucket_id + "/" + source_bucket_filename
read_storage_client = storage.Client()
bucket = read_storage_client.get_bucket(source_bucket_id)
blob = bucket.get_blob(source_bucket_filename)
data_string = blob.download_as_string()
data_string = StringIO(data_string.decode('utf-8'))
df = pd.read_csv(data_string, sep='\n', header=None)
content = ""
#partial = []
res = []
count = 0
record = 0
for i in range(len(df)):
    content +=  df.iloc[i,0] + "\n"
    #partial.append(df.iloc[i, 0] + "\n")
    count += 1
    if count == 8:
        content += "\n"
        #partial.add("\n")
        count = 0
        record += 1
    if record != 0 and record % total_records == 0:
        res.append(content)
        #res.append(''.join(partial))
        partial = []
        record = 0
        content = ""
#if len(partial) != 0:
#    res.append(''.join(partial))
#    partial = []
if content != "":
    res.append(content)
    content = ""
def upload_string_to_bucket(content, bucket_id):
    output_bucket = client.get_bucket(bucket_id)
    filename = str(uuid.uuid4()) + '.txt'
    output_blob = output_bucket.blob(filename)
    output_blob.upload_from_string(content)
    logging.info("Uploaded file: {}".format(filename))

for c in res:
    upload_string_to_bucket(c, 'input-text-data')
location_id = 'us-central1'
cloud_function_name = 'stop-compute-engine'
service = discovery.build('cloudfunctions', 'v1', cache_discovery=False)
cloud_functions_API = service.projects().locations().functions()
name = "projects/{}/locations/{}/functions/{}".format(project_name, location_id, cloud_function_name)
data = {"data" : ""}
response = cloud_functions_API.call(name=name, body=data).execute()
logging.info("Starting cloud function: {}".format(cloud_function_name))
logging.info("Response from function: {}".format(response))
