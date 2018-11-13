import re
import json
import datetime
import functools as ft
from pprint import pprint

# Global definitions
start_msg = "PXEKERNEL/PXEWAIT => PXEKERNEL/PXEWAKEUP"
end_msg   = "NORMALv2/TBSETUP => NORMALv2/ISUP" 
timegap   = 600 # In Seconds


def remove_parameters_from_msg (msg):
  re_retval = re.search(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', msg)
  if (re_retval != None):
    ip_add = re_retval.group()
    msg = msg.replace (ip_add, "IP_ADDR")
  
  re_retval = re.search (r'\w{3}\s{1,4}\d{1,2}\s{1,4}\d{2}\:\d{2}\:\d{2}',msg)
  if (re_retval != None):
    date = re_retval.group()
    msg = msg.replace (date, "DATE")
 
  re_retval = re.search (r'hp\d{1,4}' ,msg)
  if (re_retval != None):
    machine_name = re_retval.group()
    msg = msg.replace (machine_name, "MACHINE")

  print (msg)
  return msg

def get_timestamp_in_datetime (tstamp_str):
  date_time_obj = datetime.datetime.strptime (tstamp_str, '%b %d %Y %H:%M:%S')
  return date_time_obj 

def get_timestamp_in_string (msg):
  words = msg.split()
  date_str = words[0] + " " + words[1] + " 2018 " + words[2]
  date_time_obj = datetime.datetime.strptime (date_str, '%b %d %Y %H:%M:%S')
  dstr = date_time_obj.strftime ('%b %d %Y %H:%M:%S')
  return dstr 

def extract_timestamp_from_msg (hits):
  length = len(hits)
  for i in range (0, length):
    hits[i]["_source"]["timestamp"] = get_timestamp_in_string (hits[i]["_source"]["message"])

def remove_params_from_all_msg (hits):
  length = len(hits)
  for i in range (0, length):
    hits[i]["_source"]["message"] = remove_parameters_from_msg (hits[i]["_source"]["message"])

def log_compare (a, b):
  #Extract the timestamp values to datetime format from both
  ta = get_timestamp_in_datetime (a["_source"]["timestamp"])
  tb = get_timestamp_in_datetime (b["_source"]["timestamp"])
  if (ta < tb):
    #print ("Found a right aligned one")
    return -1
  elif (ta > tb):
    #print ("Found a wrong aligned one")
    return 1
  else:
    return 0

def sort_log_data_by_time (hits):
  sorted_hits = sorted (hits, key=ft.cmp_to_key (log_compare))
  return sorted_hits

def dump_all_messages (hits):
  length = len(hits)
  for i in range (0, length):
    print (hits[i]["_source"]["message"])

def get_timediff_in_seconds (a, b):
  c = b-a
  return c.seconds

# Gap is in order of seconds 
def split_by_time_window (hits, gap):
  length = len(hits)
  
  #Initialize the variables needed for loop
  new_data = []
  filename_counter = 1
  log_lines_in_file = 0
  prev_tstamp = None
  
  #Loop begins
  for i in range (0, length):
    curr_tstamp = get_timestamp_in_datetime (hits[i]["_source"]["timestamp"])

    if ((prev_tstamp == None) or (get_timediff_in_seconds (prev_tstamp, curr_tstamp) < gap)):
      #Keep adding entries 
      new_data.append(hits[i])
      log_lines_in_file += 1
    else:
      #New sequence begins here, so dump whatever we have got till now
      filename = "test" + str(filename_counter) + ".json"
      print (filename, "has", log_lines_in_file, "lines")
      with open(filename, 'w') as tmp_file:
        json.dump (new_data, tmp_file)
     
      filename_counter += 1
      new_data = None
      new_data = []
      new_data.append(hits[i])
      log_lines_in_file = 1

    prev_tstamp = curr_tstamp

  if (log_lines_in_file > 0):
    # Dump the remaining data
    filename = "test" + str(filename_counter) + ".json"
    print (filename, "has", log_lines_in_file, "lines")
    with open(filename, 'w') as tmp_file:
      json.dump (new_data, tmp_file)
    new_data = None

def split_by_start_to_end (hits, start_msg, end_msg):
  length = len(hits)

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
        new_data.append(hits[i])
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
  
  # Sort all the logs by timestamp
  data["hits"]["hits"] = sort_log_data_by_time (data["hits"]["hits"])

	# Remove the non generic values from the messages
  remove_params_from_all_msg (data["hits"]["hits"])

  #Debug dump
  #dump_all_messages (data["hits"]["hits"])

  # Call file splitter function - Splits by given start and end message
  split_by_time_window (data["hits"]["hits"], timegap)

  # Call file splitter function - Splits by given start and end message
  #split_by_start_to_end (data["hits"]["hits"], start_msg, end_msg)

# Main function starts here
with open("output.json") as rfile:
  data = json.load(rfile)

begin_preprocess (data)

