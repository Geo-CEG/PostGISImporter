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
if not shp_driver or not fgdb_driver or not pg_driver:
    raise Exception('Required GDAL driver is missing.')

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

    dbname = 'gis_data'
    hostname = 'localhost'
    username = 'gis_owner'     # NB password should be in your .pgpass file if needed.
    dataSink = None

    def __init__(self):
        pass

    def create_schema(self, schema):
        # TODO: only create if it does not already exist

        schemacmd = 'psql -c "CREATE SCHEMA %s;" -h %s -U %s --no-password --dbname=%s' % (schema, self.hostname, self.username, self.dbname)
        if not self.dryrun:
            os.system(schemacmd)
        return

    def _connstr(self):
        return "PG:host='%s' dbname='%s' active_schema=%s user='%s'" % (self.hostname, self.dbname, self.schema, self.username)
        
    def _ogr_import(shapefilename, tablename):
        """ Import a shapefile into a database table. """

        if not self.dataSink:
            try:
                dataSink = pg_driver.Open(self._connstr(), True)
            except Exception as e:
                eprint("Could not open PostGIS:", e)
                raise Exception('Could not open PostGIS connection, '+e.message)

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
    
        dprint("Copying %d features %s -> %s" % (layer.GetFeatureCount(), layerName, tablename))
        if not self.dryrun:
            pg_layer = dataSink.CopyLayer(layer, tablename)
            if pg_layer is None:
                eprint("Layer copy failed on", tablename)

        return

    def _shp2pgsql_import(self, pathname, tablename):
    
        filename,extn = os.path.splitext(pathname)
        sref_option = '-s %s' % self.sref
        if extn.lower() == '.dbf': sref_option = '-n' # This is a table not a shapefile

        # d = drop table (generates a non-fatal error if it does not exist)
        # s = spatial reference (could be -s from:to)
        # n = this is a table only, no geometry
        # I = create spatial index

        quiet = "--quiet"
        #if debug: quiet = ""

        shpcmd  = "shp2pgsql -d %s %s %s.%s" % (sref_option, pathname, self.schema, tablename)
        psqlcmd = "psql %s -h %s -U %s --no-password --dbname=%s" % (quiet, self.hostname, self.username, self.dbname)
        cmd = shpcmd + " | " + psqlcmd
        dprint(cmd)
        if not self.dryrun:
            os.system(cmd)
        return

    def _gdb_import(self, pathname, layername=None):
        if not self.dataSink:
            try:
                dataSink = pg_driver.Open(self._connstr(), True)
            except Exception as e:
                raise Exception('Could not open PostGIS connection, '+e.message)
        try:
            dataSource = fgdb_driver.Open(pathname, 0)
            if dataSource is None:
                eprint("Could not open geodatabase:", pathname)
                return
        except Exception as e:
            eprint("Could not open geodatabase:", pathfilename, e)
            return

        layer = dataSource.GetLayer(0)
        i=0
        while (layer):
            layerName = layer.GetName();
            tablename = layerName
            dprint("Copying %d features %s -> %s" % (layer.GetFeatureCount(), layerName, tablename))
            if not self.dryrun:
                pg_layer = dataSink.CopyLayer(layer, tablename)
                if pg_layer is None:
                    eprint("Layer copy failed on", tablename)
            i += 1
            layer = dataSource.GetLayer(i)
        return


    def do_import(self, list):
        """ NB: This will overwrite out any existing data!!! """

        if not self.dbname: raise Exception("database name is not set.")

        already_got_this_one = {}
        for (schema, sref, pathname, name) in list:

            if not schema: raise Exception("schema is not set.")
            if not already_got_this_one.has_key(schema):
                self.create_schema(schema)
                already_got_this_one[schema] = 1
            self.schema = schema
            self.sref = sref

            tablename = sanitize_tablename(name)

            if os.path.exists(pathname):
                filename,extn = os.path.splitext(pathname)
                dprint("Importing %s => %s" % (pathname, tablename))

                if extn == '.gdb':
                    self._gdb_import(pathname)
                    continue
                else:
                    # NOTE I currently don't have code here to handle tables
                    #_ogr_import(shapefilename, tablename)
                    # ...but shp2pgsql DOES handle DBF tables
                    self._shp2pgsql_import(pathname, tablename)
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

    # Chdir to the directory containing the list.txt file that will
    # guide this script. Then run this script.

    # You have to figure out the correct EPSG code to use for your data.
    # I suggest feeding a PRJ file to http://prj2epsg.org/search

    #sref = 2872 # NAD_1983_HARN_StatePlane_California_III_FIPS_0403_Feet
    #username = 'gis_owner'
    #password = None

    # Oh come on now put this in the list.txt file.

    sref = 2913
    username = 'gis_owner'
    hostname = 'dart.wildsong.biz'
    
# list.txt contains a list of shapefiles to import
# and optionally the table name to use in output, for example
#
# trails.shp trail
#
# imports the shapefile trails.shp into a table "trail".

    schema = None
    list = [];
    with open("list.txt") as fp:
        for item in fp.readlines():

            item = item.strip()
            if not item: continue # Skip empty lines
            if item[0]=='#': continue # Skip comments

            pair = item.split()
            try:
                pathname = pair[0]
            except RangeError as e:
                continue

            p,n = os.path.split(pathname)

            if len(pair)<2:
                # There is no tablename so create one.
                name,ext = os.path.splitext(n)
                pair.append(name)

            if pair[0].lower() == 'schema':
                schema = pair[1]
                continue
            if pair[0].lower() == 'epsg':
                sref = pair[1]
                continue

            if os.path.exists(pathname):
                list.append([schema,sref]+pair)
            else:
                eprint("Can't open '%s', skipping it." % pathname)

    dprint(list)

    imp = importer()
    #imp.dryrun = True
    
    imp.username = username
    imp.hostname = hostname
    
    imp.do_import(list);

    sys.exit(0)
