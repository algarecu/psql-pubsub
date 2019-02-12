##-----------------------------------------------------------------------------------
#--- Author: Alvaro Garcia-Recuero
#--- Date: 12th February 2018
#--- Version: 0.1
#--- Python Ver: 2.7

# -*- coding: utf-8 -*-

import paho.mqtt.client as mqtt
import time

mqttc = mqtt.Client("python_pub")
mqttc.connect("publication.trustingiot.com", 1883)

# Currently to publish the arguments are as follows
timestamp = time.time()
print timestamp
mqttc.publish("location/ethaddr", '{"latitude":-3,"longitude":10,"altitude":15,"timestamp":'+str(timestamp)+'}')
mqttc.loop(2)