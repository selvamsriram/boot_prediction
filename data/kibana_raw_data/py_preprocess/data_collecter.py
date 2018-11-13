import requests
from elasticsearch import Elasticsearch
import json

# Global variable initialization
map_file = "node_ip_mac_mapping.txt"

'''
node_id = "hp024"
node_ip_addr = "128.110.154.105"
query_str = "(source:reboot.log OR source:stated.log OR source:bootinfo.log) AND (node_id:"+node_id+"OR node_ids:"+node_id+" OR message:"+node_id+" OR message:"+node_ip_addr+") AND beat.hostname: boss.utah.cloudlab.us"

es  = Elasticsearch()
res = es.search(index="_all", scroll = '2m', body = {
     "query": {
          "query_string": {
                 "query": query_str}}, "size": 10000,}) 

sid = res['_scroll_id']

scroll_size = (res['hits']['total'])
print (scroll_size)

while scroll_size > 0:
  print ("Scrolling...")
  res = es.scroll(scroll_id=sid, scroll='2m')

  # Process current batch of hits
  #process_hits(data['hits']['hits'])

  # Update the scroll ID
  sid = res['_scroll_id']

  # Get the number of results that returned in the last scroll
  scroll_size = len(res['hits']['hits'])
  print (scroll_size)


'''
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

  print (scroll_size)
  while scroll_size > 0:
    print ("Scrolling...")
    data = es.scroll(scroll_id=sid, scroll='2m')

    # Process current batch of hits
    #process_hits(data['hits']['hits'])

    # Update the scroll ID
    sid = data['_scroll_id']

    # Get the number of results that returned in the last scroll
    scroll_size = len(data['hits']['hits'])
    print (scroll_size)
    dst_list.extend(data['hits']['hits'])

  print ("Total records collected for node", node, "is", len(dst_list))


# Function to collect data from Kibana server
def collect_data_from_kibana (map_file):
  es  = Elasticsearch()
  with open (map_file) as file:
    for line in file:
      params = line.split ()
      collect_data_per_node (es, params[0], params[1], params[2])

collect_data_from_kibana (map_file)

'''
es  = Elasticsearch()
collect_data_per_node (es, "hp024", "128.110.154.105", "98f2b3cc12e0")
'''
