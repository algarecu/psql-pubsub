##-----------------------------------------------------------------------------------
#--- Author: Alvaro Garcia-Recuero
#--- Version: 0.1
#--- Python Ver: 3.7
#-----------------------------------------------------------------------------------

# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Postgresql DB Name
DB_Name = "geomarket"
MQTT_TOPIC = "data"

# ===============================================================
# Database Manager Class

class DatabaseManager(object):
    def __init__(self):
        try:
            self.conn = psycopg2.connect(database=DB_Name, user="postgres",
                                         password="",
                                         host="localhost" )

            self.cur = self.conn.cursor()
            print("Opened database successfully")
            self.create_tables()
        except (Exception, psycopg2.DatabaseError) as error:
            raise(error)
        finally:
            logging.info('Database Manager opened successfully')

    def create_tables(self):
        query = '''create table if not exists locations (
                               facilitator varchar(90) not null, 
                               address varchar(90) not null,
                               latitude real not null,
                               longitude real not null,
                               altitude integer not null,
                               time timestamp not null,
                               geom geometry(point, 4326),
                               primary key(facilitator, address, time));'''

        query_index = 'create index if not exists idx_geom_location on locations using gist(geom)'

        self.cur.execute(query)
        self.cur.execute(query_index)
        self.conn.commit()

    def query(self, sql_query, args):
        return self.cur.execute(sql_query, args)

    def query_many(self, sql_query, args):
        return self.cur.executemany(sql_query,args)

    def close(self):
        self.conn.commit()
        self.conn.close()
        print('Database: disconnected')

