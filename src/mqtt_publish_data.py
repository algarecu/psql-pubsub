# -*- coding: utf-8 -*-
import paho.mqtt.client as mqtt

mqttc = mqtt.Client("python_pub")
mqttc.connect("geodemo.trustingiot.com", 1883)
mqttc.publish("locations", "Hello, World!")
mqttc.loop(2)