# -*- coding: utf-8 -*-
import paho.mqtt.client as mqtt

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    if rc==0:
        client.connected_flag=True #set flag
        print("connected OK Returned code=",rc)
        client.subscribe('locations/#')
    else:
        print("Bad connection Returned code= ",rc)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print "Topic: ", msg.topic+'\nMessage: '+str(msg.payload)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("geodemo.trustingiot.com", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()