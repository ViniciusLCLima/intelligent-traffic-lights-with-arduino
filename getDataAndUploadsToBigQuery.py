#!/usr/bin/env python3
import paho.mqtt.client as paho
from paho import mqtt
import json
from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
'silvestrini-ristorante-338023-4b1998ff4c2c.json')
project_id = 'silvestrini-ristorante-338023'
bigqueryClient = bigquery.Client(credentials= credentials,project=project_id)

# setting callbacks for different events to see if it works, print the message etc.
def on_connect(client, userdata, flags, rc, properties=None):
    print("CONNACK received with code %s." % rc)
# print message, useful for checking if it was successful
def on_message(client, userdata, msg):
  data = json.loads(msg.payload)
  dmlStatement = (
    "UPDATE Cars_In_Simulated_traffic_Lights.table1 "
    f"SET cross1_carsNum = cross1_carsNum + {data['cross1_carsNum']}, cross2_carsNum = cross2_carsNum + {data['cross2_carsNum']}  "
    f"WHERE hour = {data['hour']}"
  )
  print(data)
  query_job = bigqueryClient.query(dmlStatement)
  
# using MQTT version 5 here, for 3.1.1: MQTTv311, 3.1: MQTTv31
# userdata is user defined data of any type, updated by user_data_set()
# client_id is the given name of the client
client = paho.Client(client_id="", userdata=None, protocol=paho.MQTTv5)
client.on_connect = on_connect

# enable TLS for secure connection
client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
# set username and password
client.username_pw_set("vinicius", "viniciuS&7")
# connect to HiveMQ Cloud on port 8883 (default for MQTT)
client.connect("6a37b76a917148c095bbd12ab6d7a200.s1.eu.hivemq.cloud", 8883)


# setting callbacks, use separate functions like above for better visibility
client.on_message = on_message

# subscribe to all topics of encyclopedia by using the wildcard "#"
client.subscribe("carsNum", qos=1)

# loop_forever for simplicity, here you need to stop the loop manually
# you can also use loop_start and loop_stop
client.loop_forever()
