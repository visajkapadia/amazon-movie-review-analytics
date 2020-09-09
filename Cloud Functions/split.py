from googleapiclient import discovery
import os
import logging

def start_vm_instance(event, context):
     filename = event['name']
     logging.info("Dataset: {}".format(filename))

     service = discovery.build('compute', 'v1', cache_discovery=False)
     project = os.environ.get("PROJECT")
     zone = os.environ.get("ZONE")
     instance = os.environ.get("INSTANCE_NAME")
     
     request = service.instances().get(project=project, zone=zone, instance=instance)
     response = request.execute()
     fingerprint = response["metadata"]["fingerprint"]
     script = """#! /bin/bash
     sudo apt install python3-pip
     pip3 install google-cloud
     pip3 install google-cloud-storage
     pip3 install pandas
     pip3 install google
     pip3 install google-api-python-client
     python3 split.py"""
     metadata_body = {"items": [{"key": "filename", "value": filename}, {"key": "startup-script", "value": script}],"kind": "compute#metadata", "fingerprint": fingerprint}
     request = service.instances().setMetadata(project=project, zone=zone, instance=instance, body=metadata_body)
     response = request.execute()

     request = service.instances().start(project=project, zone=zone, instance=instance)
     response = request.execute()
     logging.info("file-splitter started..")

