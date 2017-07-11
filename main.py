#!/usr/bin/env python
# -*- coding: utf-8 -*-
from connection import Connection
from create_wkt import CreateWkt

import sys
import geojson
import requests
import re

class Upload():

    _con          = None
    _json_data    = None
    _multipolygon = []
    _fields       = {}
    
    def __init__(self, database, table, host, user, password, url):
        wkt               = CreateWkt(url)
        multipolygon_list = wkt.create()
        # self._con         = Connection('localhost','banco1','igor','123456')
        self._con         = Connection(host, database, user, password)
        response          = requests.get(url)
        str_decode        = response.content.decode('utf8')     
        gs_data           = geojson.loads(str_decode)

        self._json_data = gs_data['features']         
        self._formatter_multipolygon(multipolygon_list)
        self._declare_fields()
        self._prepare_fields()

    def _formatter_multipolygon(self,mp):
        for m in mp:
            mpl2 = m.replace("0,",",") 
            self._multipolygon.append(mpl2.replace("0)",")")) 

    def _declare_fields(self):
        for gj in self._json_data:
            del gj['geometry']
            for k,v in gj.items():
                if k == 'properties':
                    for key,value in gj.properties.items():
                        self._fields[key] = []
                else:
                    self._fields[k] = []

        self._fields['geometry'] = []
                    
    def _prepare_fields(self):
        for gj in self._json_data:
            for k,v in gj.items():
                if k == 'properties':
                    for key,value in gj.properties.items():
                        self._fields[key].append(value)
                else:
                    self._fields[k].append(v)

        for m in self._multipolygon:
            self._fields['geometry'].append(m)

    def prepare_sql(self,table,list_keys,tup_values):
        sql = "INSERT INTO " \
            "{}({},{},{},{},{},{},{},{},{},{},"\
            "{},{},{},{},{},{},{},{},{})"\
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"\
            "%s,%s,%s,%s,%s,%s,%s,%s,%s);".format(table,*list_keys)
        
        prepared_sql = [tup_values]
        self._con.insertion(sql,prepared_sql)

    def insert(self,table):
        jd         = self._fields 
        length     = len(jd['id'])
        count      = 0

        for i in range(length):
            tup_values = ()
            list_keys  = []
            for key,value in jd.items():
                list_keys.append(key)
                tup_values = tup_values+(value[i],)

            self.prepare_sql(table,list_keys,tup_values)
            count = count + 1

        print('{} Records Inserted.'.format(count)) 


def check_params(params):
    length_input  = len(params)
    counter = 0

    while counter < length_input:
        if params[counter] == '--table' or params[counter] == '-table':
            # result = re.findall(r'[\d]',params[counter + 1])
            if not isinstance(params[counter + 1],int):
                TABLE = params[counter + 1]

        elif params[counter] == '--host' or params[counter] == '-host':
            if isinstance(params[counter + 1],str):
                HOST = params[counter + 1]
        
        elif params[counter] == '--db' or params[counter] == '-db':
            DATABASE = params[counter + 1]
        
        elif params[counter] == '--user' or params[counter] == '-user':
            if isinstance(params[counter + 1],str):
                USER = params[counter + 1]
        
        elif params[counter] == '--pass' or params[counter] == '-pass':
            PASSWORD = params[counter + 1]
        
        elif params[counter] == '--url' or params[counter] == '-url':
            URL_GEOSERVER = params[counter + 1]

        counter = counter + 1

    u = Upload(DATABASE, TABLE, HOST, USER, PASSWORD, URL_GEOSERVER)
    u.insert(TABLE)

if __name__ == '__main__':
    check_params(sys.argv)