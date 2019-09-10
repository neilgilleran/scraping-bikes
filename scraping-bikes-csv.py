# -*- coding: utf-8 -*-
"""
This notebook is the code to download all the bike data from citybik.es API.

The csv outputted will be about 1.2Mb

Requirements::
!apt-get install autoconf automake build-essential libtool python-dev
!pip install jq
!pip install pyjq
"""

import json 
from jq import jq
import pandas as pd
from datetime import datetime
import time
import requests

#this will create a list of networks availalbe on the API
def create_network_list():
  url = 'https://api.citybik.es/v2/networks'

  response = requests.get(url)
  todos = json.loads(response.text)

  network_names = []

  for network in todos['networks']:
      network_names.append(network['id'])
  
  return network_names

#base url, we will append the network for the full address
base_url = 'https://api.citybik.es/v2/networks/'
#this query will get the data we require
jq_query = " .network.stations[] | [  .timestamp, .name, .empty_slots, .free_bikes, .extra.status , .extra.slots]"
#this wait time is used when we dont want to flood the server with requests
WAIT_TIME = 5
#we use this timestamp for the file name
DATE_TIME = datetime.now().strftime('%Y-%m-%d_%H-%M')
print(DATE_TIME)

#create a network list
networks = create_network_list()

#create the dataframe for the loop to appned the data into
bike_loop = pd.DataFrame()

#the column names we need
column_names = ['timestamp','station','slots','bikes','status','total']

""" used this to debug the following loop networks =['dublinbikes'] """

#setting up the filename
filename = 'all_citybikes_data_'+DATE_TIME+'.csv'

#need to wait every so often so we don't get a 429 error
i = len(networks)

for network in networks:
  print("Scraping: ", network)
  print(i)
  url = base_url+network
  
  with urllib.request.urlopen(url) as url:
      json_data = json.loads(url.read().decode())
  
  compressed_json = jq(jq_query).transform(json_data, multiple_output=True)
  
  df_network = pd.DataFrame(compressed_json, columns=column_names)
  df_network['network'] = network

  #TODO actually i need to get the timestamp from the API as this is localised. 
  # i will skew all the data is I apply the scrapetime as the timestamp
  #df_network['datetime'] = str(DATE_TIME)
  
  bike_loop = bike_loop.append(df_network,ignore_index=True)

  i = i - 1
  if i %10 == 0:
    print("Wait for: ", WAIT_TIME)
    time.sleep(WAIT_TIME)

try:  
  print("Creating: ", filename)
  bike_loop.to_csv(filename, index=False)  
except:
  print("There was an issue creating file: ", filename)