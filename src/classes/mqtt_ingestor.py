from __future__ import absolute_import
##-----------------------------------------------------------------------------------
# --- Version: 0.1
# --- Python Ver: 3.7
# -*- coding: utf-8 -*-

import pprint as pp
import src.classes.store as st

MQTT_TOPIC = "data"

class MQTT_Ingestor(object):
	def __init__(self, store):
		def __init__(self, store):
			self.store = store

	def on_subscribe(client, userdata):
		client.subscribe('data')

	def on_message(client, userdata, msg):
		topic = msg.topic
		m_decoded = str(msg.payload.decode("utf-8", "ignore"))
		st.store.location_data_handler(topic, m_decoded)
		pp.pprint("Topic: ", msg.topic + '\nMessage: ' + str(msg.payload))








