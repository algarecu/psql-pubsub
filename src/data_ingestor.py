##-----------------------------------------------------------------------------------
#--- Author: Alvaro Garcia-Recuero
#--- Version: 0.1
#--- Python Ver: 3.7
#-----------------------------------------------------------------------------------

import paho.mqtt.client as mqtt

import re
import classes.store as st
import argparse
import psycopg2
import logging
import requests
import os
import csv

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def file_opener(filename):
	try:
		# change inital url to localhost:8001
		url = "http://localhost:8001/files/"
		filename = filename.decode('utf-8')
		url = url + filename

		response = requests.get(url)
		print(type(response))
		with open(os.path.join(".", "buffer.csv"), 'wb') as f:
			f.write(response.content)
		f.close()
		return
	except IOError as err:
		raise(err)

def data_handler_csv(filename):
	file_opener(filename)
	with open('buffer.csv', newline='') as csv_file:
		csv_reader = csv.reader(csv_file, delimiter=';')
		for row in csv_reader:
			print(', '.join(row))
			try:
				dbObj = st.DatabaseManager()

				facilitator = row[0]
				address = row[1]
				latitude = row[2]
				longitude = row[3]
				altitude = row[4]
				time = row[5]
				time = time[0:len(row[5])-3]

				args = (facilitator, address, latitude, longitude, altitude, time, longitude, latitude)

				# Push into DB Table
				sql_query = "INSERT INTO locations (facilitator, address, latitude, longitude, altitude, time, geom)" \
							"VALUES (%s,%s,%s,%s,%s,TO_TIMESTAMP(%s::bigint), ST_SetSRID(ST_MakePoint(%s,%s),4326)::geometry);"
				print(sql_query)
				dbObj.query(sql_query, args)
				logging.info('Processing block: 0x'+str(filename))
			except (Exception, psycopg2.DatabaseError) as error:
				logging.info(error)
				raise (error)
			dbObj.close()

def data_handler_json(filename):
	jsonData = file_opener(filename)
	i = 0
	while i < len(jsonData):
		try:
			dbObj = st.DatabaseManager()

			facilitator = jsonData[i]['facilitator']
			address = jsonData[i]['address']
			latitude = jsonData[i]['latitude']
			longitude = jsonData[i]['longitude']
			altitude = jsonData[i]['altitude']
			time = jsonData[i]['timestamp']  # Because MQTT is not sending unix_timestamp in seconds use: TO_TIMESTAMP/1000

			args = (facilitator, address, latitude, longitude, altitude, time) # geom_tuple

			# Push into DB Table
			sql_query = "INSERT INTO locations (facilitator, address, latitude, longitude, altitude, time, geom_tuple) " \
						"VALUES (%s,%s,%s,%s,%s,TO_TIMESTAMP(%s::bigint/1000.0));"
			# @TODO ST_SetSRID(ST_MakePoint(%s), 4326)
			# @TODO cambiar formato timestamp
			dbObj.query(sql_query, args)
			logging.info('Processing block: 0x'+str(filename))
			i += 1
		except (Exception, psycopg2.DatabaseError) as error:
			logging.info(error)
			raise (error)
		dbObj.close()

def process_json__data(MQTT_TOPIC, the_filename):
	if re.match("data", MQTT_TOPIC):
		print("Inserting into db with topic: " + MQTT_TOPIC)
		data_handler_csv(the_filename)
	else:
		print("Wrong topic, not saving to db...")

def on_message(client, userdata, message):
	the_filename = message.payload
	process_json__data(message.topic, the_filename)

def on_publish(mqttc, obj, mid):
    print("Published to: " + str(mqttc))

def on_subscribe(mqttc, obj, mid, granted_qos):
    print("Subscribed to: " + str(mqttc) )

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	if rc == 0:
		print("connected OK Returned code=", rc)
		client.subscribe("data")
	else:
		print("Bad connection Returned code= ", rc)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(
		description='Data Ingestor',
		prog='data_ingestor.py',
		usage='%(prog)s [options]')
	parser.add_argument('--topic',
						help='topic to subscribe to',
						default='data')
	parser.print_help()

	# Parse program parameters
	try:
		args = parser.parse_args()
		topic = args.topic

		# Create client
		mqttc = mqtt.Client()

		# Assign event callback
		mqttc.on_connect = on_connect
		mqttc.on_message = on_message
		mqttc.connect("localhost", 1883, 60)
		mqttc.subscribe(topic, 1)
		mqttc.loop_forever()
	except Exception as e:
		logger.error(e)

