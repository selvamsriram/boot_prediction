import re
import json
import datetime
import functools as ft
import requests
from elasticsearch import Elasticsearch
from pprint import pprint

# Global definitions
start_msg = "PXEKERNEL/PXEWAIT => PXEKERNEL/PXEWAKEUP"
end_msg   = "NORMALv2/TBSETUP => NORMALv2/ISUP" 
timegap   = 600 # In Seconds
base_time = datetime.datetime.strptime('10Sep2018', '%d%b%Y')
map_file = "node_ip_mac_mapping.txt"
months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
positive_pattern = ["PXEKERNEL/PXEBOOTING => PXEKERNEL/PXEWAIT", "NORMALv2/TBSETUP => NORMALv2/ISUP"]

#------------------------------------------------------------------------------
# Label is +ve if the last 5 lines contains any of the +ve pattern strings
def find_label (data_list):
  length = len (data_list)
  i = length-1

  # Assuming that no proper state transmission will happen in 5 lines
  if (length < 5):
    return False

  while (i > (length-6)):
    for pattern in positive_pattern:
      if pattern in data_list[i]["_source"]["message"]:
        return True
    i -= 1
  return False

def remove_parameters_from_msg (msg):
  re_retval = re.search(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', msg)
  if (re_retval != None):
    ip_add = re_retval.group()
    msg = msg.replace (ip_add, "IP_ADDR")
  
  re_retval = re.findall (r'\w{3}\s{1,4}\d{1,2}\s{1,4}\d{2}\:\d{2}\:\d{2}',msg)
  if (re_retval != None):
    for date in re_retval:
      msg = msg.replace (date, "DATE")
  
  re_retval = re.findall (r'hp\d{1,4}' ,msg)
  if (re_retval != None):
    for machine_name in re_retval:
      msg = msg.replace (machine_name, "MACHINE")
 
  re_retval = re.findall (r'ms\d{1,5}' ,msg)
  if (re_retval != None):
    for machine_name in re_retval:
      msg = msg.replace (machine_name, "MACHINE")
 
  return msg

#------------------------------------------------------------------------------
def get_timestamp_in_datetime (tstamp_str):
  date_time_obj = datetime.datetime.strptime (tstamp_str, '%b %d %Y %H:%M:%S')
  return date_time_obj 

#------------------------------------------------------------------------------
def get_timestamp_in_string (msg):
  words = msg.split()
  year = 2018

  if (words[0] not in months):
    dstr = "Jun 05 2018 16:25:28"
    return dstr
  elif ((words[0] == "Feb") and (words[1] == "29")):
    year = 2016

  date_str = words[0] + " " + words[1] + " " + str(year) + " " + words[2]
  date_time_obj = datetime.datetime.strptime (date_str, '%b %d %Y %H:%M:%S')
  dstr = date_time_obj.strftime ('%b %d %Y %H:%M:%S')
  return dstr 

#------------------------------------------------------------------------------
def extract_timestamp_from_msg (hits):
  length = len(hits)
  for i in range (0, length):
    hits[i]["_source"]["timestamp"] = get_timestamp_in_string (hits[i]["_source"]["message"])

#------------------------------------------------------------------------------
def remove_params_from_all_msg (hits):
  length = len(hits)
  for i in range (0, length):
    hits[i]["_source"]["message"] = remove_parameters_from_msg (hits[i]["_source"]["message"])

#------------------------------------------------------------------------------
def log_compare (a, b):
  #Extract the timestamp values to datetime format from both
  ta = get_timestamp_in_datetime (a["_source"]["timestamp"])
  tb = get_timestamp_in_datetime (b["_source"]["timestamp"])
  if (ta < tb):
    return -1
  elif (ta > tb):
    return 1
  else:
    return 0

#------------------------------------------------------------------------------
def sort_log_data_by_time (hits):
  sorted_hits = sorted (hits, key=ft.cmp_to_key (log_compare))
  return sorted_hits

#------------------------------------------------------------------------------
def dump_all_messages (hits):
  length = len(hits)
  for i in range (0, length):
    print (hits[i]["_source"]["message"])

#------------------------------------------------------------------------------
def get_timediff_in_seconds (a, b):
  c = b-a
  return c.seconds

#------------------------------------------------------------------------------
# Gap is in order of seconds 
def split_by_time_window (hits, gap, node_id):
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
      filename = "test_" + node_id + "_" +str(filename_counter) + ".json"
      #Decide the label
      new_data[0]["_source"]["label"] = find_label (new_data) 
      print (filename, "has", log_lines_in_file, "lines, label", new_data[0]["_source"]["label"])
       
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
    filename = "test_" + node_id + "_" + str(filename_counter) + ".json"
    new_data[0]["_source"]["label"] = find_label (new_data) 
    print (filename, "has", log_lines_in_file, "lines, label", new_data[0]["_source"]["label"])
    with open(filename, 'w') as tmp_file:
      json.dump (new_data, tmp_file)
    new_data = None

#------------------------------------------------------------------------------
def split_by_start_to_end (hits, start_msg, end_msg, node_id):
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
        filename = "test_" + node_id + "_" +str(filename_counter) + ".json"
        with open(filename, 'w') as tmp_file:
          json.dump (new_data, tmp_file)

        filename_counter += 1
        new_data = None
        new_data = []
        log_lines_in_file = 0

    if (start == True):
     new_data.append(hits[i])
     log_lines_in_file += 1

#------------------------------------------------------------------------------
def eliminate_older_msgs (hits):
  length = len (hits)

  new_hits = []
  for i in range (0, length):
    curr_tstamp = get_timestamp_in_datetime (hits[i]["_source"]["timestamp"])
    if (curr_tstamp < base_time):
      continue
    else:
      new_hits.append (hits[i])

  return new_hits

#------------------------------------------------------------------------------
# Preprocess master function
def begin_preprocess (data, node_id):
  if data == None:
    return

  # Since timestamp isn't present in all json objects extract from the message
  extract_timestamp_from_msg (data)
 
  # Eliminate messages before 10 Sept 2018
  data = eliminate_older_msgs (data)
   
  # Sort all the logs by timestamp
  data = sort_log_data_by_time (data)

	# Remove the non generic values from the messages
  remove_params_from_all_msg (data)

  #Debug dump
  #dump_all_messages (data)

  # Call file splitter function - Splits by given start and end message
  split_by_time_window (data, timegap, node_id)

  # Call file splitter function - Splits by given start and end message
  #split_by_start_to_end (data, start_msg, end_msg, node_id)

#------------------------------------------------------------------------------
# Collect data per node from Kibana
def collect_data_per_node (es, node, ip_addr, mac_addr):
  print ("Querying node ",node, "IP Addr ",ip_addr, "Mac Addr ",mac_addr)
  query_str = "(source:reboot.log OR source:stated.log OR source:bootinfo.log) AND (node_id:"+node+"OR node_ids:"+node+" OR message:"+node+" OR message:"+ip_addr+") AND beat.hostname: boss.utah.cloudlab.us"
  data = es.search(index="_all", scroll = '2m', body = {
                                                       "query": {
                                                         "query_string": {
                                                           "query": query_str}}, "size": 10000,})
  sid = data['_scroll_id']
  scroll_size = (data['hits']['total'])

  # Save retrieved information
  dst_list = []
  dst_list.extend(data['hits']['hits'])

  while scroll_size > 0:
    data = es.scroll(scroll_id=sid, scroll='2m')

    # Process current batch of hits
    #process_hits(data['hits']['hits'])

    # Update the scroll ID
    sid = data['_scroll_id']

    # Get the number of results that returned in the last scroll
    scroll_size = len(data['hits']['hits'])
    dst_list.extend(data['hits']['hits'])

  print ("Total records collected for node", node, "is", len(dst_list))
  return dst_list


#------------------------------------------------------------------------------
# Function to collect data from Kibana server
def collect_data_from_kibana (map_file):
  es  = Elasticsearch()
  i = 0
  with open (map_file) as file:
    for line in file:
      if (i > 200):
        break
      params = line.split ()
      if (not ((params[0] == "hp034") or (params[0] == "hp035") or (params[0] == "hp036") or (params[0] == "hp037") or (params[0] == "hp038"))):
        continue

      collected_data = collect_data_per_node (es, params[0], params[1], params[2])
      begin_preprocess (collected_data, params[0])
      i += 1

#------------------------------------------------------------------------------
# Main function starts here
collect_data_from_kibana (map_file)

#es  = Elasticsearch()
#collected_data = collect_data_per_node (es, "hp035", "128.110.154.115", "98f2b3cc8370")
#begin_preprocess (collected_data, "hp035")

