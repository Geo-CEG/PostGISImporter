#!/usr/bin/env python
#
#   Generic script for importing GIS data into PostGIS
#
from __future__ import print_function
import sys, os
from osgeo import ogr

shp_driver = ogr.GetDriverByName('ESRI Shapefile')
fgdb_driver = ogr.GetDriverByName('OpenFileGDB') # There is an ESRI driver too but this one is bundled with GDAL.
pg_driver = ogr.GetDriverByName('PostgreSQL')

debug = False

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
        
def dprint(*args, **kwargs):
    if debug:
        print(*args, file=sys.stderr, **kwargs)
        
def sanitize_tablename(tablename):
    cleaned = tablename.lower().strip().replace(' ','_')
    return cleaned
    
class importer(object):

    #  There are two methods for import included here,
    #  _ogr_import() calls the osgeo gdal ogr code
    #  _shp2pgsql_import() uses a subprocess to call shp2pgsql.
    #  You have to pick one or the other in do_import().
    #
    #  TODO: evaluate the merits and say why one is better than the other.
    #

    # If set to true just set up commands, don't do database writes
    dryrun = False

    sref = 3857 # Web Mercator is a default, you should pick something

    schema = None              # you have to set this
    dbname = 'gis_data'
    hostname = 'localhost'
    username = 'gis_owner'     # NB password should be in your .pgpass file if needed.
    dataSink = None

    def __init__(self, schema=None, sref=3857):
        self.schema = schema
        self.sref = sref
        pass

    def create_schema(self):
        # TODO: only create if it does not already exist

        password = self.password
        if password == None:
            password_arg = '--no-password' # Try running w/o a password.

        schemacmd = 'psql -c "CREATE SCHEMA %s;" -h %s -U %s --no-password --dbname=%s' % (self.schema, self.hostname, self.username, self.dbname)
        if not self.dryrun:
            os.system(schemacmd)
        return

    def _ogr_import(shapefilename, tablename):
        """ Import a shapefile into a database table. """

        connstr  = "PG:dbname='%s' active_schema=%s username='%s'" % (self.dbname, self.schema, self.username)
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
    
        dprint("Copying %d features from %s to %s" % (layer.GetFeatureCount(), layerName, connstr + ' ' + tablename))
        if not self.dryrun:
            pg_layer = dataSink.CopyLayer(layer, tablename)
            if pg_layer is None:
                eprint("Layer copy failed on", tablename)

        return

    def _shp2pgsql_import(self, shapefilename, tablename):
    
        # d = drop table (generates a non-fatal error if it does not exist)
        # s = spatial reference (could be -s from:to)

        shpcmd  = "shp2pgsql -d -s %d %s %s.%s" % (self.sref, shapefilename, self.schema, tablename)
        psqlcmd = "psql --quiet -h %s -U %s --no-password --dbname=%s" % (self.hostname, self.username, self.dbname)
        cmd = shpcmd + " | " + psqlcmd
        dprint(cmd)
        if not self.dryrun:
            os.system(cmd)
        return

    def do_import(self, list):
        """ NB: This will overwrite out any existing data!!! """

        if not self.dbname: raise Exception("database name is not set.")
        if not self.schema: raise Exception("schema is not set.")

        self.create_schema()
    
        for (shapefilename, name) in list:
            tablename = sanitize_tablename(name)
            if os.path.exists(shapefilename):
                dprint("Importing %s => %s" % (shapefilename, tablename))
                #_ogr_import(shapefilename, tablename)
                self._shp2pgsql_import(shapefilename, tablename)
            else:
                eprint("Can't find %s" % pathname)
        return

####################################################

def test_sanitizer():
    l_test = [ "MixedCase", "Spaces in name" ]
    for i in l_test:
        print("'%s' => '%s'" % (i, sanitize_tablename(i)))

if __name__ == '__main__':

    debug  = True

    # You have to figure out the correct EPSG code to use for your data.
    # I suggest feeding a PRJ file to http://prj2epsg.org/search

    #d_county = '/home/GISData/CA/Marin' # directory containing the source data
    #sref = 2872 # NAD_1983_HARN_StatePlane_California_III_FIPS_0403_Feet
    #schema   = "ca_co_marin"
    #username = 'gis_owner'
    #password = None

    d_county = '/Users/bwilson/ownCloud/DataRepository/OR/LincolnCounty'
    sref = 2913
    schema = 'or_co_lincoln'
    username = os.environ['GISOWNER']
    hostname = 'dart.wildsong.biz'
    
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
                    name,ext = os.path.splitext(pair[0])
                    pair.append(name)
                list.append(pair)
            else:
                eprint("There is no file '%s', skipping it." % pair[0])

    dprint(list)

    imp = importer(schema=schema, sref=sref)
    #imp.dryrun = True
    
    # Using default for username and don't need a password
    self.username = username
    self.hostname = hostname
    
    imp.do_import(list);

    sys.exit(0)
