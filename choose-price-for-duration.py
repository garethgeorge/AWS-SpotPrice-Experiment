import subprocess 
import sys

def data_file_name(az, inst_type):
  return "./data/%s:%s.Linux.temp2" % (az, inst_type)

def xypair_file_name(az, inst_type):
  return "./data/%s:%s.xypairs" % (az, inst_type)

def fetch_data(az, inst_type):
  cmd = "sh download-latest-data.sh \"%s\" \"%s.Linux.temp2\"" % (az, inst_type)
  print("Fetch data from server - exec: " + cmd)
  p = subprocess.Popen(cmd, shell=True)
  p.wait()
  return data_file_name(az, inst_type)

def generate_predictions(az, inst_type):
  cmd = "sh make-predictions.sh %s %s" % (data_file_name(az, inst_type), xypair_file_name(az, inst_type))
  print("Generating predictions - exec: " + cmd)
  p = subprocess.Popen(cmd, shell=True)
  p.wait()
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
  fetch_data("us-east-1a", "m4.large")
  generate_predictions("us-east-1a", "m4.large")
  closest_duration, price = predict_price_for_duration("us-east-1a", "m4.large", 3.0)
  print("%f, %f" % (closest_duration, price))
  sys.exit(0)
except Exception as e:
  raise e 
  sys.exit(1)