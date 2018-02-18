from aws import ec2, ec2_factory 
import shutil
import os
import itertools 
from datetime import datetime, timedelta
import subprocess 
import math 
import json

regions = ["us-east-1", "us-west-2"]
inst_types = ["m4.large", "t2.large", "c1.medium", "m4.4xlarge", "m4.10xlarge", "r4.xlarge"]

datetime_today = datetime.now()
datetime_2weeksago = datetime_today - timedelta(days=60)
datetime_2weeksago_epoch = int(datetime_2weeksago.strftime("%s"))

def download_data(az, inst_type):
  print("\t\tDownloading data for az %s inst_type %s" % (az, inst_type))

  p = subprocess.Popen("sh ./scripts/download-latest-data.sh \"%s\" \"%s.data\"" % (az, inst_type), shell=True)
  p.wait()

  path = "./data/%s:%s.data" % (az, inst_type)
  if os.path.exists(path):
    with open(path, "r") as f:
      list = []
      for line in f.readlines():
        line = line.strip()
        line = line.split("\t")
        if len(line) != 6: continue 
        
        timestamp = int(datetime.strptime(line[5], "%Y-%m-%dT%H:%M:%S.000Z").strftime("%s"))
        price = float(line[4])
        list.append((timestamp, price))
      return sorted(set(list))
  else:
    return None 

def aws_get_data(region, az, inst_type):
  global datetime_2weeksago
  ec2_r = ec2_factory(region)

  spot_price_history = []
  paginator = ec2_r.get_paginator('describe_spot_price_history')
  for result in paginator.paginate(
        StartTime = datetime_2weeksago,
        EndTime = datetime_today,
        InstanceTypes = [inst_type],
        AvailabilityZone = az,
      ):
    spot_price_history.extend([(int(r["Timestamp"].strftime('%s')), float(r["SpotPrice"])) for r in result['SpotPriceHistory'] if r["ProductDescription"] == "Linux/UNIX"])
    spot_price_history = sorted(set(spot_price_history))
  
  return spot_price_history 

for region in regions:
  ec2_r = ec2_factory(region)

  print("\nDeobfuscating region %s" % region)
  az_list = [az["ZoneName"] for az in ec2_r.describe_availability_zones()["AvailabilityZones"]]
  print("\tAZ's in region: %s" % (", ".join(az_list)))

  aws_history = {}
  rich_history = {}

  for az in az_list:
    for inst_type in inst_types:
      print("\t\tPulling data for AZ: %s InstType: %s" % (az, inst_type))
      aws_history[(az, inst_type)] = aws_get_data(region, az, inst_type) or []
      rich_history[(az, inst_type)] = download_data(az, inst_type) or []
      print("\t\t\tPulled %d datapoints from aws" % len(aws_history[(az, inst_type)]))
      print("\t\t\tPulled %d datapoints from Rich's server" % len(rich_history[(az, inst_type)]))

  print("\tcomputing comparison matrix")
  similarity_matrix = {}
  for az_aws, az_rich, inst_type in itertools.product(az_list, az_list, inst_types):
    print("\t\tcomparing %s with %s" % (az_aws, az_rich))
    _aws_history = aws_history[(az_aws, inst_type)]
    _rich_history = rich_history[(az_rich, inst_type)]

    if (az_aws, az_rich) not in similarity_matrix:
      similarity_matrix[(az_aws, az_rich)] = 0
    
    delta = 0

    print("\t\t%d samples from aws and %d samples from rich to be used" % (len(_aws_history), len(_rich_history)))
    if len(_rich_history) > 0:
      print("\t\t\tsample of a row from rich history %s" % (_rich_history[-1],))
    if len(_aws_history) > 0:
      print("\t\t\tsample of a row from aws history %s" % (_aws_history[-1],))
    
    _rich_history = list(filter(lambda x: x[0] > datetime_2weeksago_epoch, _rich_history))

    for aws_ts, aws_price in _aws_history:
      for rich_ts, rich_price in _rich_history:
        if aws_ts == rich_ts:
          if aws_price == rich_price:
            delta += 1
          else:
            delta -= 1

    similarity_matrix[(az_aws, az_rich)] += delta

  for key, similarity in similarity_matrix.items():
    az_aws, az_rich = key 
    print("\t%s vs %s: %d" % (az_aws, az_rich, similarity))

  print("\tResulting mappings:")
  try:
    with open("az_mapping.json", "r") as f:
      az_mapping = json.load(f)
  except:
    az_mapping = {}
  for az_aws in az_list:
    max_val = max((similarity_matrix[(az_aws, az_rich)], az_aws) for az_rich in az_list)
    print("\t\t" + str(az_aws) + " - " + str(max_val))
    az_mapping[az_aws] = max_val[1]
  
  with open("az_mapping.json", "w") as f:
    json.dump(az_mapping, f, indent=2)
