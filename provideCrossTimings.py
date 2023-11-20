from datetime import datetime
from time import sleep
import paho.mqtt.client as paho
from paho import mqtt
from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
'silvestrini-ristorante-338023-4b1998ff4c2c.json')
project_id = 'silvestrini-ristorante-338023'
bigqueryClient = bigquery.Client(credentials= credentials,project=project_id)

# setting callbacks for different events to see if it works, print the message etc.
def on_connect(client, userdata, flags, rc, properties=None):
    print("CONNACK received with code %s." % rc)

# with this callback you can see if your publish was successful
def on_publish(client, userdata, mid, properties=None):
    print("mid: " + str(mid))


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
client.on_publish = on_publish


client.loop_start()
while True:
  actualHour = datetime.now().hour
  print(actualHour)
  SQL_Statement = f"""
  SELECT cross1_carsNum, cross2_carsNum
  From Cars_In_Simulated_traffic_Lights.table1
  WHERE hour = {actualHour}
  """
  query_job = bigqueryClient.query(SQL_Statement)
  results = list(query_job.result())
  cross1_carsNum = results[0]["cross1_carsNum"]
  cross2_carsNum = results[0]["cross2_carsNum"]
  totalCars = cross1_carsNum + cross2_carsNum
  if totalCars == 0:
    cross1OpenTime = cross2OpenTime = 45
  else:
    cross1OpenTime = 10+70*(cross1_carsNum/totalCars)
    cross2OpenTime = 90 - cross1OpenTime
  # a single publish, this can also be done in loops, etc.
  client.publish("openTimings", payload='{' + f'"cross1":{cross1OpenTime}, "cross2": {cross2OpenTime}'+'}', qos=1)
  print('{' + f'"cross1":{cross1OpenTime}, "cross2": {cross2OpenTime}'+'}')
  sleep(5)
client.loop_stop
# loop_forever for simplicity, here you need to stop the loop manually
# you can also use loop_start and loop_stop
