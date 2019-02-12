##-----------------------------------------------------------------------------------
#--- Author: Alvaro Garcia-Recuero
#--- Date: 12th February 2018
#--- Version: 0.1
#--- Python Ver: 2.7

# -*- coding: utf-8 -*-

import paho.mqtt.client as mqtt
from store_data_to_db import location_data_handler

# Subscriber to: http://publication.trustingiot.com/location/test/

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    if rc==0:
        client.connected_flag=True #set flag
        print("connected OK Returned code=",rc)
        client.subscribe('location/#')

        ## For multiple topics, uncomment this and make the callback with userdata
        # if isinstance(userdata['topics'], list):
        #     for topic in userdata['topics']:
        #         client.subscribe(topic)
        # else:
        #     client.subscribe(userdata['topics'])

    else:
        print("Bad connection Returned code= ",rc)


def on_message(client, userdata, msg):
    """
    Function is a callback for when a PUBLISH message is received from the server.

    Args:
        client:
        userdata: the userdata for callback
        msg: the data
    Returns:
        none: none
    """
    print "Topic: ", msg.topic+'\nMessage: '+str(msg.payload)
    location_data_handler(msg.topic, msg.payload)


def on_subscribe(client, userdata, mid, granted_qos):
    pass

# Assign event callback
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Connect
client.connect("publication.trustingiot.com", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()