import json
from pprint import pprint
import os.path
import datetime
import pre_process as pre_p

key_mapping = {}

def convert_json_to_libsvm (filename, log_filename):
  ret = os.path.exists (filename)
  if (ret == False):
    return False

  temp_file = open ("temp.train", "w")

  with open(filename) as f:
    data = json.load(f)

  len_data = len(data)
  #For now hard-coded to +1
  label = '+1'
  prev_s = ''
  prev_tstamp = None

  for log_line in range (len_data):
    if (data[log_line]['_source']['message'] not in key_mapping):
      key_mapping[data[log_line]['_source']['message']] = len (key_mapping.keys()) + 1

    curr_tstamp = pre_p.get_timestamp_in_datetime (data[log_line]['_source']['timestamp'])
    if (prev_tstamp != None):
      time_diff = pre_p.get_timediff_in_seconds (prev_tstamp, curr_tstamp)
    else:
      time_diff = 0

    prev_tstamp = curr_tstamp

    print (str (time_diff))

    key = key_mapping[data[log_line]['_source']['message']]
    time_val = " " + str(1) + ":" + str(time_diff) 

    prev_s = prev_s + " " + str(log_line + 2) + ":" + str(key)

    s = label + time_val + prev_s
    temp_file.write (s +"\n")

  temp_file.close () 

  t = open ("temp.train", "r")

  with open (log_filename, "a+") as log:
    log.write (t.read ())

  return True


#Main execution
#Total big file which will be the input to the ML classifier
log = open ("logfile.train", "w")

i = 1
ret = True

while (ret != False):
  ret = convert_json_to_libsvm ('test'+str(i)+'.json', 'logfile.train')
  i += 1

log.close()
for i,val in key_mapping.items():
  print (val, i)


