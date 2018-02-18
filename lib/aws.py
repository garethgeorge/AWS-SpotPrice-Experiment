import yaml
import boto3 
import os
import time
from lib.logs import log

# http://boto3.readthedocs.io/en/latest/guide/migrationec2.html#stopping-terminating-instances
# really useful information

with open('./config.yml') as f:
  config = yaml.load(f)

def ec2_factory(region='us-west-1'):
  global config 
  return boto3.client(
    'ec2', 
    region_name=region,
    aws_access_key_id=config['AWS']['KeyId'], 
    aws_secret_access_key=config['AWS']['SecretAccessKey']
  )

def request_spot_instance(ec2, az, inst_type, image, price):
  log("Requesting instance (%s, %s, %s, %f)" % (az, inst_type, image, price))
  try:
    result = ec2.request_spot_instances(
      InstanceCount=1,
      LaunchSpecification={
        "ImageId": image, # Ubuntu Server AMI Image
        "InstanceType": inst_type,
        "Placement": {
          "AvailabilityZone": az
        },
      },
      SpotPrice=str(price),
      InstanceInterruptionBehavior='terminate' # terminate if interrupted
    )
    sirId = result["SpotInstanceRequests"][0]["SpotInstanceRequestId"]
    ec2.create_tags(
      Resources=[
        sirId
      ],
      Tags = [
        {
          "Key": "Name",
          "Value": "RACELab EC2 SLA Experimental Instance",
        },
      ]
    )

    # wait until the instance comes online
    log("\twaiting until the instance comes online.")
    waiter = ec2.get_waiter('spot_instance_request_fulfilled')
    waiter.wait(
      Filters=[
        {
          "Name": "state",
          "Values": ["active"]
        },
      ],
      SpotInstanceRequestIds=[sirId],
    )

    return sirId
  except:
    log("Encountered error when creating a spot instance request.")
    log(traceback.format_exc())
  
  return None 

def get_active_or_open_spot_requests(ec2):
  instance_ids = [r["SpotInstanceRequestId"] for r in ec2.describe_spot_instance_requests(
    Filters=[
      {
        "Name": "state",
        "Values": ["open", "active"]
      }
    ],
  )["SpotInstanceRequests"]]
  return instance_ids

def shutdown_spot_market_requests(ec2, spot_request_ids):
  if len(spot_request_ids) == 0: return None

  # get the running requests
  instance_ids = [id for sir, id in get_instances_for_spotrequests(ec2, spot_request_ids).items()]

  log("Terminating instance ids: %s" % (", ".join(instance_ids)))
  terminating_instance_ids = [r["InstanceId"] for r in ec2.terminate_instances(
    InstanceIds=instance_ids
  )["TerminatingInstances"]]
  
  assert(set(instance_ids) == set(terminating_instance_ids))

  log("Canceling spot instance requests: %s" % (", ".join(spot_request_ids)))
  return ec2.cancel_spot_instance_requests(
    SpotInstanceRequestIds=spot_request_ids,
  )

def get_instances_for_spotrequests(ec2, spot_request_ids):
  # get info about the requestss
  spot_instance_requests = ec2.describe_spot_instance_requests(
    SpotInstanceRequestIds=spot_request_ids
  )["SpotInstanceRequests"]

  # return a map of spot_request_id to instance id
  return {
    r["SpotInstanceRequestId"]: r["InstanceId"] 
    for r in spot_instance_requests if "InstanceId" in r
  }
