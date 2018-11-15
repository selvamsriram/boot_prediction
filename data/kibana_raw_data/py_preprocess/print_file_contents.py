import json

def dump_file (filename):
  with open(filename) as f:
    data = json.load(f)

  len_data = len(data)
  for log_line in range (len_data):
    print (data[log_line]['_source']['timestamp'], data[log_line]['_source']['message'])


while 1:
  filename = input("Enter filename: ")
  filename = filename + ".json"
  dump_file (filename)
