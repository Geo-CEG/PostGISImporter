#!/usr/bin/env python -u
#
#   Generic script for importing GIS data into PostGIS
#   Not so generic rules for importing USDA SSURGO data.
#
from __future__ import print_function
import sys, os
from osgeo import ogr
import psycopg2
from psycopg2.extensions import AsIs
import csv

shp_driver = ogr.GetDriverByName('ESRI Shapefile')
fgdb_driver = ogr.GetDriverByName('OpenFileGDB') # There is an ESRI driver too but this one is bundled with GDAL.
pg_driver = ogr.GetDriverByName('PostgreSQL')
csv_driver = ogr.GetDriverByName('CSV')
if not shp_driver:
    raise Exception('Required GDAL Shapefile driver is missing.')
if not fgdb_driver:
    raise Exception('Required GDAL OpenFileGDB driver is missing.')
if not pg_driver:
    raise Exception('Required GDAL PostgreSQL driver is missing.')

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

    # If set to true just set up commands, don't do database writes
    dryrun = False
    dryrun = True

    sref = 3857 # Web Mercator is a default, you should pick something

    dbname = 'gis_data'
    hostname = 'localhost'
    username = 'gis_owner'     # NB password should be in your .pgpass file if needed.
    dataSink = None
    overwrite = True
    
    # SSURGO special variables
    dcolumn = {} # indexed by table, contains column definitions
    dtype = {} # variable types collected from TXT file, used to map SSURGO types to SQL types
    
    def __init__(self):
        pass

    def create_schema(self, schema):
        dsn = "host='%s' dbname='%s' user='%s'" % (self.hostname, self.dbname, self.username)
        with psycopg2.connect(dsn) as conn:
            cursor = conn.cursor()
            try:
                result = cursor.execute("""CREATE SCHEMA %s""", (AsIs(schema),))
                pass
            except psycopg2.ProgrammingError as e:
                if e.message.find('already exists') == -1:
                    raise e
                pass
        return

    def _pg_connect(self):
        connstr = "PG:host='%s' dbname='%s' active_schema=%s user='%s'" % (self.hostname, self.dbname, self.schema, self.username)
        try:
            dataSink = pg_driver.Open(connstr, True)
        except Exception as e:
            raise Exception('Could not open PostGIS connection, '+e.message)
        return dataSink

    def prj2epsg(prj_file):
        srs = osr.SpatialReference()
        with open(shapeprj_path, 'r') as prj_file:
            prj_txt = prj_file.read()
        srs.ImportFromESRI([prj_txt])
        dprint('Shape prj is: %s' % prj_txt)
        dprint('WKT is: %s' % srs.ExportToWkt())
        dprint('Proj4 is: %s' % srs.ExportToProj4())
        srs.AutoIdentifyEPSG()
        return srs.GetAuthorityCode(None)
        
    def _vector_import(self, pathname, tablename):
        """ Import a feature class into a database table.
        shapefilename can have an extension of .shp or .dbf;
        if you give it a dbf file it will be treated as tabular data. """

        dataSink = self._pg_connect()
        options = []
        if self.overwrite: options.append("OVERWRITE=YES")

        # Let's figure out the projection.
        prjname,ext = os.path.splitext(pathname)
        sref = prj2epsg(prjname+'.prj')
        print('sref = %s' % sref)
        try :
            dataSource = shp_driver.Open(pathname, 0)
            if dataSource is None:
                eprint("Could not open file:", pathname)
                return
        except Exception as e:
            eprint("Exception: Could not open file:", pathname, e)
            return

        layer = dataSource.GetLayer(0)
        layerName = layer.GetName();
    
        feature_count = layer.GetFeatureCount()
        if feature_count:
            dprint("Shapefile: %d features %s -> %s.%s" % (feature_count, layerName, self.schema, tablename))
            if not self.dryrun:
                pg_layer = dataSink.CopyLayer(layer, tablename, options)
                if pg_layer is None:
                    eprint("Copy failed on", tablename)
        else:
            dprint("Skipping, no features: '%s'" % layerName)
        
        return

    def _gdb_import(self, pathname, layername=None):
        dataSink = self._pg_connect()
        try:
            dataSource = fgdb_driver.Open(pathname, 0)
            if dataSource is None:
                eprint("Could not open geodatabase:", pathname)
                return
        except Exception as e:
            eprint("Could not open geodatabase:", pathfilename, e)
            return
        options = []
        if self.overwrite: options.append("OVERWRITE=YES")
        layer = dataSource.GetLayer(0)
        i=0
        while (layer):
            layername = layer.GetName();
            tablename = layername
            feature_count = layer.GetFeatureCount()
            if feature_count:
                dprint("FGDB %d features %s -> %s.%s" % (feature_count, layername, self.schema,tablename))
                if not self.dryrun:
                    pg_layer = dataSink.CopyLayer(layer, tablename, options)
                    if pg_layer is None:
                        eprint("Copy failed on '%s'" % tablename)
            else:
                dprint("FGDB: skipping empty '%s'" % layername)
            i += 1
            layer = dataSource.GetLayer(i)
        return
    
    def read_table_definitions(self, file):
        """ This is informative but I am not really using it yet. """
        table_defn = [ 'basename', 'alt_name', 'descriptive_name', 'description', 'short_name' ]
        with open(file, "r") as fp:
            rdr = csv.reader(fp, delimiter='|')
            for row in rdr:
                self.dtable[row[0]] = (row[2],row[3])
        return
    
    def _read_column_definitions(self, folder, file):
        """ Read an mstabcol.txt file and build a dictionary,
        each key is a table name and it contains a list of variables in that table. """
        if self.dcolumn:
            return True # done already!
        
        boolval = False
        with open(os.path.join(folder, file), "r") as fp:
            rdr = csv.reader(fp, delimiter='|')
            for row in rdr:
                table = row[0]
        
                # For column definitions see mdstattabcols.txt
                # each tuple contains columnseq,varname, vartype,notnull,fieldsize, and wordy description
        
                tuple = (row[1],row[2],row[3],  row[5],row[6],row[7],  row[13])
                self.dtype[row[5]] = row[6]
                try:
                    self.dcolumn[table].append(tuple)
                except KeyError:
                    self.dcolumn[table] = [tuple]
            boolval = True
        return boolval
        
    def _csv_import(self, pathname, table):
        dsn = "host='%s' dbname='%s' user='%s'" % (self.hostname, self.dbname, self.username)
        conn = psycopg2.connect(dsn)
        cursor = conn.cursor()
        fp = open(pathname, "r")
        cursor.copy_from(fp, table, sep='|')
        return
    
    def _ssurgo_import(self, folder):
        """ Import all the SSURGO data tables from a folder. """
        dsn = "host='%s' dbname='%s' user='%s'" % (self.hostname, self.dbname, self.username)

        lut = {'Integer':   """BIGINT""",
               'Vtext':     """TEXT""",
               'Boolean':   """BOOLEAN""",
               'Choice':    """VARCHAR(%s)""",
               'String':    """VARCHAR(%s)""",
                'Float':     """FLOAT(8)""",
               'Date/Time': """TIMESTAMP""",}
        
        self._read_column_definitions(folder, 'mstabcol.txt')

        with psycopg2.connect(dsn) as conn:
            cursor = conn.cursor()
            
            for table in self.dcolumn:
                query = """CREATE TABLE %s.%s (\n"""
                params = [AsIs(self.schema),AsIs(table)]
                insert_params = ""
                addcomma=False
                tablepath = os.path.join(folder, table+'.txt')
                if not os.path.exists(tablepath):
                    continue
                for col in self.dcolumn[table]:
                    index    = col[0]
                    phyname  = col[1]
                    logname  = col[2]
                    typecol  = col[3]
                    notnull  = col[4]
                    size     = col[5]
                    desc     = col[6]
                    sqltype  = lut[typecol] # this can throw errors if lut is missing an entry
                    #print(col)
                    try:
                        tdef = sqltype % size
                    except TypeError:
                        tdef = sqltype # This happens when size is None and is normal

                    if addcomma: query += ',\n'
                    query += "  %s %s"
                    if notnull == 'Yes':
                        query += " NOT NULL"
                    
                    name=phyname
                    if phyname=='notnull':
                        name = logname
                    params += [AsIs(name), AsIs(tdef)]
                    if addcomma: insert_params += ','
                    insert_params += name
                    addcomma = True
                    pass
                query += ");"
                #print(cursor.mogrify(query, params))
                rval = cursor.execute("DROP TABLE IF EXISTS %s.%s;", (AsIs(self.schema),AsIs(table),))
                rval = cursor.execute(query, params)

                with open(tablepath, "r") as fp:
                    csv_reader = csv.reader(fp, delimiter='|')
                    for row in csv_reader:
                        query = """INSERT INTO %s.%s VALUES (""" + ','.join(["%s"]*len(row)) + ");"
                        
                        # Replace empty strings with NULL to avoid type conversion problem
                        nulled=[]
                        for element in row:
                            if not element:
                                nulled.append(AsIs('NULL'))
                            else:
                                nulled.append(element)
                                
                        params = [AsIs(schema), AsIs(table),]+nulled
                        #code = cursor.mogrify(query, params)
                        #print(code)
                        rval = cursor.execute(query,params)
                        pass
                pass
                #cursor.copy_from(fp, table, sep='|')

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

            if sref == 'ssurgo':
                self._ssurgo_import(pathname)
                continue
            else:
                self.sref = sref
                tablename = sanitize_tablename(name)

                filename,extn = os.path.splitext(pathname)
                if extn == '.gdb':
                    self._gdb_import(pathname)
                elif extn == '.csv' or extn == '.txt':
                    self._csv_import(pathname, tablename)
                else:
                    self._vector_import(pathname, tablename)
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
    file = "list.txt"
#    file = "ssurgo.txt"
    with open(file,'r') as fp:
        for item in fp.readlines():

            item = item.strip()
            if not item: continue # Skip empty lines
            if item[0]=='#': continue # Skip comments

            pair = item.split()
            try:
                pathname = pair[0]
            except RangeError as e:
                continue
            
            if pair[0] == 'exit': break

            p,n = os.path.split(pathname)

            if len(pair)<2:
                # There is no tablename so create one.
                name,ext = os.path.splitext(n)
                pair.append(name)

            first = pair[0].lower()
            if first == 'schema':
                schema = pair[1]
                continue
            elif first == 'epsg':
                sref = pair[1]
                continue
            elif first == 'ssurgo':
                list.append([schema,'ssurgo',pair[1],None])
            else:
                #if os.path.exists(pathname):
                list.append([schema,sref]+pair)

    #dprint(list)

    imp = importer()
    imp.overwrite = False
    imp.username = username
    imp.hostname = hostname
    
    imp.do_import(list);
    
    print("importer has completed.")

    sys.exit(0)
