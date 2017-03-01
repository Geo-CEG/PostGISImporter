#! /usr/bin/env python
from __future__ import print_function
import sys
from osgeo import osr

def esriprj2standards(shapeprj_path):
   prj_file = open(shapeprj_path, 'r')
   prj_txt = prj_file.read()
   srs = osr.SpatialReference()
   srs.ImportFromESRI([prj_txt])
   print('Shape prj is: %s' % prj_txt)
   print('WKT is: %s' % srs.ExportToWkt())
   print('Proj4 is: %s' % srs.ExportToProj4())
   srs.AutoIdentifyEPSG()
   return srs.GetAuthorityCode(None)

if __name__ == "__main__":
   # This code even works sometimes.
   
   # I expect an argument that is the name of a PRJ file.
   # I try to turn it into an EPSG code for you.

   prj = sys.argv[1]
   epsg = esriprj2standards(prj)

   print("EPSG code is '%s'" % epsg)
