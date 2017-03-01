#!/usr/bin/env python
#
#   Generic script for importing GIS data into PostGIS
#
from __future__ import print_function
import sys, os
from osgeo import ogr

# If set to true just set up commands, don't do database writes
dryrun = True

shp_driver = ogr.GetDriverByName('ESRI Shapefile')
fgdb_driver = ogr.GetDriverByName('OpenFileGDB') # There is an ESRI driver too but this one is bundled with GDAL.
pg_driver = ogr.GetDriverByName('PostgreSQL')

debug = True

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
        
def dprint(*args, **kwargs):
    if debug:
        dprint(*args, file=sys.stderr, **kwargs)
        
class importer(object):

    #  There are two methods for import included here,
    #  _ogr_import() calls the osgeo gdal ogr code
    #  _shp2pgsql_import() uses a subprocess to call shp2pgsql.
    #  You have to pick one or the other in do_import().
    #
    #  TODO: evaluate the merits and say why one is better than the other.
    #

    sref = 3857 # Web Mercator is a default, you should pick something

    schema = None              # you have to set this

    username = 'gis_owner'     # These should come from environment
    password = 'easypassword'
    dbname = 'gisdata'

    dataSink = None

    def __init__(self, schema=None, sref=3857):
        pass

    def _ogr_import(shapefilename, db):
        """ Import a shapefile into a database table. """

        connstr  = "PG:dbname='%s' user='%s' password='%s' active_schema=%s" % (self.dbname, self.username, self.password, self.schema)
        if not self.dataSink:
            try:
                dataSink = pg_driver.Open(connstr, True)
            except Exception as e:
                eprint("Could not open PostGIS:", e)
                dprint(connstr)
                return

        try :
            dataSource = shp_driver.Open(shapefilename, 0)
            if dataSource is None:
                eprint("Could not open file geodatabase:", shapefilename)
                return
        except Exception as e:
            eprint("Exception: Could not open shapefile:", shapefilename, e)
            return

        layer = dataSource.GetLayer(0)
        layerName = layer.GetName();
    
        dprint("Copying %d features from %s to %s" % (layer.GetFeatureCount(), layerName, connstr + ' ' + db))
        if not dryrun:
            pg_layer = dataSink.CopyLayer(layer, db)
            if pg_layer is None:
                eprint("Layer copy failed on", db)

        return

    def _shp2pgsql_import(self, shapefilename, db):
    
        # d = drop table (generates a non-fatal error if it does not exist)
        # s = spatial reference (could be -s from:to)
    
        shpcmd  = "shp2pgsql -d -s %d %s %s.%s" % (sref, shapefilename, schema, db)
        psqlcmd = "psql --quiet --username=%s --no-password --dbname=%s" % (username, dbname)
        cmd = shpcmd + " | " + psqlcmd
        dprint(cmd)
        os.system(cmd)
        return

    def do_import(self, list):

        for (shapefilename, db) in list:
            if not len(db): db = shapefilename[:-4].lower()
            pathname = shapefilename # os.path.join(dir, shapefilename)
            if os.path.exists(pathname):
                dprint("Importing %s => %s" % (pathname, db))
                #_ogr_import(shapefilename, db)
                _shp2pgsql_import(shapefilename, db)
            else:
                eprint("Can't find %s" % pathname)
    return

####################################################

if __name__ == '__main__':
    
    sref = 102243 # NAD_1983_HARN_StatePlane_California_III_FIPS_0403_Feet

    d_county = '/home/GISData/CA/Marin' # directory containing the source data
    dbname   = 'gisdata'
    schema   = "ca_co_marin"
    username = "gis_owner"
    password = "easypassword"

    os.chdir(d_county)

# list.txt contains a list of shapefiles to import
# and optionally the table name to use in output, for example
#
# trails.shp trail
#
# imports the shapefile trails.shp into a table "trail".

    list = [];
    with open("list.txt") as fp:
        for item in fp.readlines():
            pair = item.split()
            if os.path.exists(pair[0]):
                if len(pair)<2:
                    pair.append(pair[0])
                list.append(pair)
            else:
                eprint("There is no file '%s', skipping it." % pair[0]

    dprint(list)

    imp = importer(schema=schema, sref=sref)
    imp.do_import(list);

    sys.exit(0)
