import json
from pprint import pprint
import os.path

key_mapping = {}

def convert_json_to_libsvm (filename, log_filename):
  ret = os.path.exists (filename)
  if (ret == False):
    return

  temp_file = open ("temp.train", "w")

  with open(filename) as f:
    data = json.load(f)

  len_data = len(data)
  #For now hard-coded to +1
  label = '+1'
  prev_s = ''

  for log_line in range (len_data):
    if (data[log_line]['_source']['message'] not in key_mapping):
      key_mapping[data[log_line]['_source']['message']] = len (key_mapping.keys()) + 1

    key = key_mapping[data[log_line]['_source']['message']]

    prev_s = prev_s + " " + str(log_line + 2) + ":" + str(key)

    s = label + prev_s
    temp_file.write (s +"\n")

  temp_file.close () 

  t = open ("temp.train", "r")

  with open (log_filename, "a+") as log:
    log.write (t.read ())


#Main execution
no_of_files = 20

#Total big file which will be the input to the ML classifier
log = open ("logfile.train", "w")

for i in range (1, no_of_files):
  convert_json_to_libsvm ('test'+str(i)+'.json', 'logfile.train')

log.close()


