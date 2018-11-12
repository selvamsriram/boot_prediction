import json
import datetime
from pprint import pprint

# Global definitions
start_msg = "PXEKERNEL/PXEWAIT => PXEKERNEL/PXEWAKEUP"
end_msg   = "NORMALv2/TBSETUP => NORMALv2/ISUP" 

def get_timestamp_in_datetime (msg):
  words = msg.split()
  date_str = words[0] + " " + words[1] + " 2018 " + words[2]
  date_time_obj = datetime.datetime.strptime (date_str, '%b %d %Y %H:%M:%S')
  dstr = date_time_obj.strftime ('%b %d %Y %H:%M:%S')
  return dstr 

def extract_timestamp_from_msg (hits):
  length = len(hits)
  for i in range (0, length):
    hits[i]["_source"]["timestamp"] = get_timestamp_in_datetime (hits[i]["_source"]["message"])
  
def split_by_start_to_end (hits, start_msg, end_msg):
  length = len(hits)
  print (length)

  #Initialize the variables needed for loop
  new_data = []
  start = False
  filename_counter = 1
  log_lines_in_file = 0

  for i in range (0, length):
    # Begin data collection when the state moves from PXEWAIT to PXEWAKEUP
    if (start_msg in hits[i]["_source"]["message"]):
      print ("Data collection started")
      start = True
    
    # End data collection when the state moves from TBSETUP to ISUP
    elif (end_msg in hits[i]["_source"]["message"]):
      print ("Data collection ended", log_lines_in_file, "Lines in this iteration")
      start = False

      if (log_lines_in_file != 0):
        #Write all the data here
        filename = "test" + str(filename_counter) + ".json"
        with open(filename, 'w') as tmp_file:
          json.dump (new_data, tmp_file)

        filename_counter += 1
        new_data = None
        new_data = []
        log_lines_in_file = 0

    if (start == True):
     new_data.append(hits[i])
     log_lines_in_file += 1


# Preprocess master function
def begin_preprocess (data):
  if data["hits"] == None:
    return
  if data["hits"]["hits"] == None:
    return

  # Since timestamp isn't present in all json objects extract from the message
  extract_timestamp_from_msg (data["hits"]["hits"])
  
  # Call file splitter function
  split_by_start_to_end (data["hits"]["hits"], start_msg, end_msg)

# Main function starts here
with open("output.json") as rfile:
  data = json.load(rfile)

begin_preprocess (data)


# Old but gold
'''
def begin_preprocess (data):

  if data["hits"] == None:
    return

  if data["hits"]["hits"] == None:
    return

  length = len(data["hits"]["hits"])
  print (length)

  new_data = []
  start = False
  filename_counter = 1
  log_lines_in_file = 0
  for i in range (0, length):
    if ("PXEKERNEL/PXEWAIT => PXEKERNEL/PXEWAKEUP" in data["hits"]["hits"][i]["_source"]["message"]):
      #print (data["hits"]["hits"][i]["_source"]["message"])
      print ("Data collection started")
      start = True
    elif ("NORMALv2/TBSETUP => NORMALv2/ISUP" in data["hits"]["hits"][i]["_source"]["message"]):
      print ("Data collection ended", log_lines_in_file, "Lines in this iteration")
      start = False

      if (log_lines_in_file != 0):
        #Write all the data here
        filename = "test" + str(filename_counter) + ".json"
        with open(filename, 'w') as tmp_file:
          json.dump (new_data, tmp_file)

        filename_counter += 1
        new_data = None
        new_data = []
        log_lines_in_file = 0

    if (start == True):
        new_data.append(data["hits"]["hits"][i])
        log_lines_in_file += 1
'''

