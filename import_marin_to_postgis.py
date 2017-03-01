#!/usr/bin/env python

import sys, os
from osgeo import ogr
from glob import glob

# If set to true just set up commands, don't do database writes
dryrun = True

shp_driver = ogr.GetDriverByName('ESRI Shapefile')
fgdb_driver = ogr.GetDriverByName('FileGDB')
pg_driver = ogr.GetDriverByName('PostgreSQL')

sref = 102243 # NAD_1983_HARN_StatePlane_California_III_FIPS_0403_Feet

d_county = 'Marin' # subdirectory containing the source data
dbname   = 'gisdata'
schema   = "ca_co_marin"
username = "gis_owner"
password = "easypassword"
connstr  = "PG:dbname='%s' user='%s' password='%s' active_schema=%s" % (dbname, username, password, schema)

def do_ogr_import(shapefilename, db):
    try :
        dataSource = shp_driver.Open(shapefilename, 0)
        if dataSource is None:
            print "Could not open file geodatabase:", shapefilename
            return
    except:
        print "Exception: Could not open shapefile:", shapefilename
        return

    layer = dataSource.GetLayer(0)
    layerName = layer.GetName();
    
    print "Copying %d features from %s to %s" % (layer.GetFeatureCount(), layerName, connstr + ' ' + db)
    if not dryrun:
        pg_layer = dataSink.CopyLayer(layer, db)
        if pg_layer is None:
            print "Layer copy failed on", db

    return

def do_shp2pgsql_import(shapefilename, db):
    
    # d = drop table (generates a non-fatal error if it does not exist)
    # s = spatial reference (could be -s from:to)
    
    shpcmd  = "shp2pgsql -d -s %d %s %s.%s" % (sref, shapefilename, schema, db)
    psqlcmd = "psql --quiet --username=%s --no-password --dbname=%s" % (username, dbname)
    cmd = shpcmd + " | " + psqlcmd
    #print cmd
    os.system(cmd)
    return

def do_import(dir, list):
    os.chdir(dir)
    for (shapefilename, db) in list:
        if not len(db): db = shapefilename[:-4].lower()
        pathname = shapefilename # os.path.join(dir, shapefilename)
        if os.path.exists(pathname):
            print "Importing %s => %s" % (pathname, db)
#            do_ogr_import(shapefilename, db)
            do_shp2pgsql_import(pathname, db)
        else:
            print "Can't find %s" % pathname
    return

####################################################

try:
    dataSink = pg_driver.Open(connstr, True)
except Exception:
    print "Could not open PostGIS:", connstr


fp=open(os.path.join(d_county, "list.txt"))
list = [];
for item in fp.readlines():
    pair = item.split()
    if len(pair)<2:
        pair.append(pair[0])
    list.append(pair)

print list

do_import("Marin", list);

sys.exit(0)
