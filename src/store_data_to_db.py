##-----------------------------------------------------------------------------------
#--- Author: Alvaro Garcia-Recuero
#--- Date: 12th February 2018
#--- Version: 0.1
#--- Python Ver: 2.7
#-----------------------------------------------------------------------------------

# -*- coding: utf-8 -*-

import json
import psycopg2
import re

# Postgresql DB Name
DB_Name = "geodb"

# ===============================================================
# Database Manager Class

class DatabaseManager():
    def __init__(self):
        self.conn = psycopg2.connect(database=DB_Name, user="geodbuser", password="", host="127.0.0.1", port="5432")
        self.cur = self.conn.cursor()
        print "Opened database successfully"


    def add_del_update_db_record(self, sql_query, args=()):
        print "Entering db_record method..."
        print sql_query
        print args

        self.cur.execute(sql_query, args)
        self.conn.commit()
        self.cur.close()
        return

    # def __del__(self):
    #     self.cur.close()
    #     self.conn.close()


# ===============================================================
# Functions to push Sensor Data into Database

# Function to save Temperature to DB Table
def data_handler(userid, jsonData):
    # Parse Json Data
    json_data = json.loads(jsonData)

    latitude = json_data['latitude']
    longitude = json_data['longitude']
    altitude = json_data['altitude']
    timestamp = json_data['timestamp']

    print userid, latitude, longitude, altitude, timestamp
    myuserid = str(userid)

    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("INSERT INTO location_data (user_id,latitude,longitude,altitude,timestamp) VALUES (%s,%s,%s,%s,%s);", (myuserid, latitude, longitude, altitude, timestamp))
    # del dbObj
    print "Inserted location data into database."
    print ""


# ===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

def location_data_handler(inputtopic, jsonData):
    if re.match(r'^location', inputtopic):
        topic, userid = inputtopic.split("/")

        print "After split I print 3 rows:"
        print topic
        print userid

        parsed_json = json.loads(jsonData)
        print json.dumps(parsed_json, indent=4, sort_keys=True)
        data_handler(userid, jsonData)
    else:
        print "Wrong topic, not saving to db..."

# ===============================================================