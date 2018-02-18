import subprocess 
import sys
import argparse 

parser = argparse.ArgumentParser(description="A program that estimates how much you should bid for a given duration")

parser.add_argument("az", help="The availability zone in which the instance will be placed.")
parser.add_argument("inst_type", help="The instance type to be requested.")
parser.add_argument("duration", type=float, help="The duration for which the instance is requested.")

args = parser.parse_args()

def data_file_name(az, inst_type):
  return "./data/%s:%s.Linux.temp2" % (az, inst_type)

def xypair_file_name(az, inst_type):
  return "./data/%s:%s.xypairs" % (az, inst_type)

def fetch_data(az, inst_type):
  cmd = "sh ./scripts/download-latest-data.sh \"%s\" \"%s.Linux.temp2\"" % (az, inst_type)
  print("Fetch data from server - exec: " + cmd)
  p = subprocess.Popen(cmd, shell=True)
  p.wait()
  return data_file_name(az, inst_type)

def generate_predictions(az, inst_type):
  cmd = "sh ./scripts/make-predictions.sh %s %s" % (data_file_name(az, inst_type), xypair_file_name(az, inst_type))
  print("Generating predictions - exec: " + cmd)
  p = subprocess.Popen(cmd, shell=True)
  p.wait()
  if p.returncode != 0:
    sys.exit(p.returncode)
  return xypair_file_name(az, inst_type)

def parse_predictions(az, inst_type):
  with open(xypair_file_name(az, inst_type), "r") as f:
    xypairs_data = map(lambda x: x.split(" "), f.read().split("\n"))
    xypairs_data = filter(lambda x: len(x) == 2, xypairs_data)
    xypairs_data = map(lambda x: (float(x[0]), float(x[1])), xypairs_data)
    xypairs_data = sorted(set(xypairs_data)) # incase of duplicates. Yep yep.
  return xypairs_data

def predict_price_for_duration(az, inst_type, desired_duration):
  xypairs_data = parse_predictions(az, inst_type)
  return next((duration, value) for duration, value in xypairs_data if duration >= desired_duration)

try:
  fetch_data(args.az, args.inst_type)
  generate_predictions(args.az, args.inst_type)
  closest_duration, price = predict_price_for_duration(args.az, args.inst_type, args.duration)
  print("%f, %f" % (closest_duration, price))
  sys.exit(0)
except Exception as e:
  raise e 
  sys.exit(1)
