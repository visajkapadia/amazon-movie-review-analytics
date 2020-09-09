from googleapiclient import discovery
import os
import logging

def stop_compute_engine(req):
     service = discovery.build('compute', 'v1', cache_discovery=False)
     project = os.environ.get("PROJECT")
     zone = os.environ.get("ZONE")
     instance = os.environ.get("INSTANCE_NAME")
     request = service.instances().stop(project=project, zone=zone, instance=instance)
     response = request.execute()
     logging.info("VM instance API called!")


