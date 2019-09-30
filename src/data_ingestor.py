##-----------------------------------------------------------------------------------
#--- Author: Alvaro Garcia-Recuero
#--- Version: 0.1
#--- Python Ver: 3.7
#-----------------------------------------------------------------------------------

import paho.mqtt.client as mqtt

import re
import json
import classes.store as st
import logging
import psycopg2

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

MQTT_TOPIC = "data"

def file_opener(filename):
	try:
		with open(filename, 'rb') as json_file:
			parsed_json = json.load(json_file)
		json_file.close()
		return parsed_json
	except IOError as err:
		raise(err)

# Function to save location to DB Table
def data_handler(filename):
	jsonData = file_opener(filename)
	print(jsonData)
	i = 0
	while i < len(jsonData):
		try:
			dbObj = st.DatabaseManager()

			facilitator = jsonData[i]['facilitator']
			address = jsonData[i]['address']
			latitude = jsonData[i]['latitude']
			longitude = jsonData[i]['longitude']
			altitude = jsonData[i]['altitude']
			time = jsonData[i]['time']  # Because MQTT is not sending unix_timestamp in seconds use: TO_TIMESTAMP/1000

			args = (facilitator, address, latitude, longitude, altitude, time)

			# Push into DB Table
			sql_query = "INSERT INTO locations (facilitator, address, latitude, longitude, altitude, time) " \
						"VALUES (%s,%s,%s,%s,%s,TO_TIMESTAMP(%s::bigint/1000.0));"

			dbObj.query(sql_query, args)
			logging.info('Processed data into DB ')
			i += 1
		except (Exception, psycopg2.DatabaseError) as error:
			logging.info(error)
			raise (error)
		dbObj.close()

def process_json__data(MQTT_TOPIC, the_filename):
	if re.match("data", MQTT_TOPIC):
		print("Inserting into db with topic: " + MQTT_TOPIC)
		data_handler(the_filename)
	else:
		print("Wrong topic, not saving to db...")

def on_message(client, obj, message):
	# the_message = json.loads(message.payload)
	the_filename = message.payload
	process_json__data(message.topic, the_filename)
	# the_message_decoded = str(message.payload.decode("utf-8","ignore"))

def on_publish(mqttc, obj, mid):
    print("Published to: " + str(mqttc))

def on_subscribe(mqttc, obj, mid, granted_qos):
    print("Subscribed to: " + str(mqttc) )

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	print("Enter connect")
	if rc == 0:
		print("connected OK Returned code=", rc)
		client.subscribe("data")
	else:
		print("Bad connection Returned code= ", rc)

if __name__ == '__main__':
	mqttc = mqtt.Client()

	# Assign event callback
	mqttc.on_connect = on_connect
	mqttc.on_message = on_message

	mqttc.connect("localhost", 1883, 60)
	mqttc.subscribe("data", 1)
	mqttc.loop_forever()

