##-----------------------------------------------------------------------------------
#--- Author: Alvaro Garcia-Recuero
#--- Version: 0.1
#--- Python Ver: 3.7
#-----------------------------------------------------------------------------------

# -*- coding: utf-8 -*-

import paho.mqtt.client as mqtt
import argparse
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bang the MQTT broker with a JSON!',
                                     prog='data_publisher.py',
                                     usage='%(prog)s [options]')
    parser.add_argument('--filename', help='id of the file to reply to mqtt',
                        default='config/geoblock.json')
    parser.add_argument('--topic',
                        help='topic to publish to',
                        default='data')
    parser.print_help()

    try:
        args = parser.parse_args()
        filename = args.filename
        topic = args.topic
    except Exception as e:
        logger.error(e)

    mqttc = mqtt.Client("python_publisher")
    mqttc.connect("localhost", 1883)
    ret = mqttc.publish(topic, payload=filename, qos=0, retain=False)
    mqttc.loop()