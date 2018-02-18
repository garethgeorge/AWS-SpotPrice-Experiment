import argparse 
import os
from datetime import datetime, timedelta
import subprocess
import sys
import json
import traceback
import time 
from lib.logs import *
from lib.aws import *

parser = argparse.ArgumentParser(description="Run an experiment renting instances from AWS over a configured duration of time")
parser.add_argument('region', help='the region in which to aquire the instances')
parser.add_argument('az', help='the az in the region from which to get the instances')
parser.add_argument('duration', type=float, help='how long to aquire the server for')
parser.add_argument('times', type=int, help='how many times to repeat the test')
parser.add_argument('inst_type', help='the type of instance to request')
parser.add_argument('image', help='The linux image to use when creating the instance. ami-f2d3638a is a good default for us-west-2')
args = parser.parse_args()
assert(args.region in args.az) # the region is always a substring of the az

log("\n\nRunning experiment with arguments: ")
log("\tRegion: %s" % args.region)
log("\tAZ: %s" % args.az)
log("\tDuration: %d hours per instance ordered" % args.duration)
log("\tTimes: %d instances to be ordered" % args.times)
log("\tInstance Type: %s" % args.inst_type)
log("\tTotal expected runtime: %d * %d = %d hours" % (args.duration, args.times, args.duration * args.times))

ec2 = ec2_factory(args.region)


# 
# log out settings & confirm user is okay with estimated cost
# 
def guestimate_bid_price(az, inst_type, duration):
  p = subprocess.Popen("python3 ./choose-price-for-duration.py \"%s\" \"%s\" %.3f" % (args.az, args.inst_type, args.duration), shell=True, stdout=subprocess.PIPE)
  p.wait()
  if p.returncode != 0:
    raise Exception("failed to properly generate a bid price guess")
  last_line = p.stdout.read().decode('utf-8').split("\n")[-2]
  last_line_vals = list(map(lambda x: float(x.strip()), last_line.split(", ")))
  return last_line_vals[0], last_line_vals[1]

print("Working on a prediction of how much this experiment will cost (worst case)")
duration, price = guestimate_bid_price(args.az, args.inst_type, args.duration)
log("\tCurrent recommended bid price: %.4f, extrapolated most likely worst case total cost: $%.4f" % (price, price * args.duration * args.times))
log("\t\tPrice guaranties duration: %.4f" % duration)

# we will terminate the experiment if it looks like we will exceed this cost
upper_bound_cost_limit = price * args.duration * args.times * 4 # DO NOT ALLOW THE EXPERIMENT TO EXCEED THIS COST

user_confirmation = input("Please confirm that this is OKAY before continuing execution (type 'yes'): ")
if user_confirmation != "yes":
  print("execution cancelled by user.")
  sys.exit(0)


#
# begin rent and confirm loop!
#

# request_spot_instance(args.az, args.inst_type, args.image, price)
print("dumping an initial list of spot instance requests.")
print(debug_print_dict(ec2.describe_spot_instance_requests(
  Filters=[
    {
      "Name": "state",
      "Values": ["active", "open"]
    }
  ]
)))

# print("Shutting down instances...")
# print(debug_print_dict(shutdown_spot_market_instances()))

# print("new states!")
# print(debug_print_dict(ec2.describe_spot_instance_requests(
#   Filters=[
#     {
#       "Name": "state",
#       "Values": ["active", "open"]
#     }
#   ]
# )))


cycle_no = 0 
while True:
  try:
    cycle_no += 1
    log("Cycle number %d" % cycle_no)
    
    log("First fetching active requests and shutting them down before beginning the next cycle.")
    current_requests = get_active_or_open_spot_requests(ec2)
    log("\tcurrently active spot market requests: %s" % (", ".join(current_requests)))
    log("Shutting those instances down.")
    log(debug_print_dict(shutdown_spot_market_requests(ec2, current_requests)))

    log("Waiting 30 seconds to let shutdown requests to fully process.")
    time.sleep(30)
    log("Done waiting, computing price to bid for a new %s instance for duration %d" % (args.inst_type, args.duration))
    duration, price = guestimate_bid_price(args.az, args.inst_type, args.duration)
    log("\tCurrent recommended bid price: %.4f, extrapolated most likely worst case total cost: $%.4f" % (price, price * args.duration * args.times))
    log("\t\tPrice guaranties duration: %.4f" % duration)
    
    log("Requesting instance.")
    spotmarket_request_id = request_spot_instance(ec2, args.az, args.inst_type, args.image, price)
    log("Successfully requested the instance, sleeping 2 seconds")
    time.sleep(2)

    request_info = ec2.describe_spot_instance_requests(
      SpotInstanceRequestIds=[spotmarket_request_id]
    )

    log(debug_print_dict(request_info))
    
    log_result(
      debug_dict({
        "CycleNumber": cycle_no,
        "Timestamp": datetime.now().strftime("%s"),
        "Type": "created spot instance request",
        "RequestInfo": request_info,
        "Message": "requested an instance of %s at price %f, will shutdown in duration %d" % (args.inst_type, price, args.duration)
      })
    )

    log("Cycle %d Going to sleep for %f hours" % (cycle_no, args.duration))
    # time.sleep(args.duration * 3600)
    time.sleep(120)
    log("Woke up after a long nap!")

    request_info = ec2.describe_spot_instance_requests(
      SpotInstanceRequestIds=[spotmarket_request_id]
    )
    log_result(
      debug_dict({
        "CycleNumber": cycle_no,
        "Timestamp": datetime.now().strftime("%s"),
        "Type": "polling spot instance request",
        "RequestInfo": request_info,
        "Message": "duration limit reached. Polling for status information before shutdown",
      })
    )

    # now that we are done sleeping & have logged out the spot request information
    # we should also log out the instance's status just for completeness
    instance_ids = [id for sir, id in get_instances_for_spotrequests(ec2, [spotmarket_request_id]).items()]

    if len(instance_ids) == 0:
      log_result(
        debug_dict({
          "CycleNumber": cycle_no,
          "Timestamp": datetime.now().strftime("%s"),
          "Type": "polling EC2 machine",
          "RequestInfo": {
            "error": "not found, there was no InstanceId property set on the spotrequest",
          },
          "Message": "did not find any instance associated with this spotmarket request",
        })
      )
    else:
      log_result(
        debug_dict({
          "CycleNumber": cycle_no,
          "Timestamp": datetime.now().strftime("%s"),
          "Type": "polling EC2 machine",
          "RequestInfo": ec2.describe_instances(InstanceIds=instance_ids),
          "Message": "returned information for the instance that was associated with this spotmarket request",
        })
      )

    log("Ending cycle number %d" % cycle_no)
  except:
    log("ERROR!!! ENCOUNTRED AN UNHANDLED ERROR")
    log(traceback.format_exc())

    log("Shutting down currently active requests to prevent wasteage.")
    current_requests = get_active_or_open_spot_requests(ec2)
    log("\tcurrently active spot market requests: %s" % (", ".join(current_requests)))
    log("Shutting those instances down.")
    log(debug_print_dict(shutdown_spot_market_requests(ec2, current_requests)))

    log("Sleeping for an hour before trying again next cycle.")
    time.sleep(3600)