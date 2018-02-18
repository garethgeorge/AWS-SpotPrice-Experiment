import os
import json
from datetime import datetime, timedelta

log_file = "logs/logs.txt"
results_file = "logs/results.txt"

def log(*argz):
  global log_file

  if not os.path.exists("logs"):
    os.mkdir("logs")

  message = " ".join(map(str, argz))
  print(message)
  with open(log_file, "a") as f:
    f.write(str(datetime.now()) + "\t" + message + "\n")

def log_result(data):
  with open(results_file, "a") as f:
    f.write(json.dumps(data) + "\n")

def debug_dict(d):
  def helper(d):
    t = type(d)
    if t == list:
      return [helper(v) for v in d]
    elif t == dict:
      return {k: helper(v) for k,v in d.items()}
    elif t == int or t == float or t == bool or t == str:
      return d
    else:
      return str(d)
  return helper(d)
def debug_print_dict(d):
  return json.dumps(debug_dict(d), indent=2)

