##-----------------------------------------------------------------------------------
#--- Author: Alvaro Garcia-Recuero
#--- Version: 0.1
#--- Python Ver: 3.7
#-----------------------------------------------------------------------------------

# -*- coding: utf-8 -*-

import paho.mqtt.client as mqtt
import json
import argparse
import logging
import pprint

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
pp = pprint.PrettyPrinter(indent=4)

MQTT_TOPIC = "data"

# Main
if __name__ == '__main__':
    # Add arguments
    parser = argparse.ArgumentParser(description='Bang the MQTT broker with a JSON!',
                                     prog='data_publisher.py',
                                     usage='%(prog)s [options]')
    parser.add_argument('--filename', help='Pass name of payload in JSON format',
                        default='config/geoblock.json')
    parser.print_help()

    # Parse authentication details
    try:
        args = parser.parse_args()
        filename = args.filename
        print(filename)
    except Exception as e:
        logger.error(e)

    mqttc = mqtt.Client("python_publisher")
    mqttc.connect("localhost", 1883)
    ret = mqttc.publish(MQTT_TOPIC, payload=filename, qos=0, retain=False)
    mqttc.loop()
