from osgeo import ogr

import pygeoj
import geojson
import requests
import sys


class CreateWkt():

    _geo_json     = None
    _wkt          = None
    _multipolygon = []

    def __init__(self,url):
        response       = requests.get(url)
        str_decode     = response.content.decode('utf8')
        self._geo_json = geojson.loads(str_decode)
    
    def _add_geometry(self):
        geometry = []
        for gj in self._geo_json['features']:
            geometry.append(gj['geometry']['coordinates'][0][0])
        return geometry

    def create(self):
        geometry = self._add_geometry()

        for gj in geometry:
            self._wkt = ogr.Geometry(ogr.wkbMultiPolygon)
            ring      = ogr.Geometry(ogr.wkbLinearRing)
            poly      = ogr.Geometry(ogr.wkbPolygon) 
            
            for coord in gj:
                ring.AddPoint(coord[0],coord[1])
            
            poly.AddGeometry(ring)
            self._wkt.AddGeometry(poly)
            self._multipolygon.append(self._wkt.ExportToWkt())
        
        return self._multipolygon    
