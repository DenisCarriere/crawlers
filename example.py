import os
from pymongo.connection import Connection
from progressbar import ProgressBar
from osgeo import ogr

def shape2mongodb(shape_path, mongodb_server, mongodb_port, mongodb_db, mongodb_collection, append, query_filter):
	"""Convert a shapefile to a mongodb collection"""
    print ' Converting a shapefile to a mongodb collection '
    driver = ogr.GetDriverByName('ESRI Shapefile')
    print 'Opening the shapefile %s...' % shape_path
    ds = driver.Open(shape_path, 0)
    if ds is None:
        print 'Can not open', ds
        sys.exit(1)
    lyr = ds.GetLayer()
    totfeats = lyr.GetFeatureCount()
    lyr.SetAttributeFilter(query_filter)
    print 'Starting to load %s of %s features in shapefile %s to MongoDB...' % (lyr.GetFeatureCount(), totfeats, lyr.GetName())
    print 'Opening MongoDB connection to server %s:%i...' % (mongodb_server, mongodb_port)
    connection = Connection(mongodb_server, mongodb_port)
    print 'Getting database %s' % mongodb_db
    db = connection[mongodb_db]
    print 'Getting the collection %s' % mongodb_collection
    collection = db[mongodb_collection]
    if append == False:
        print 'Removing features from the collection...'
        collection.remove({})
    print 'Starting loading features...'
    # define the progressbar
    pbar = ProgressBar(maxval=lyr.GetFeatureCount()).start()
    k=0
    # iterate the features and access its attributes (including geometry) to store them in MongoDb
    feat = lyr.GetNextFeature()
    while feat:
        mongofeat = {}
        geom = feat.GetGeometryRef()
        mongogeom = geom.ExportToWkt()
        mongofeat['geom'] = mongogeom
        # iterate the feature's  fields to get its values and store them in MongoDb
        feat_defn = lyr.GetLayerDefn()
        for i in range(feat_defn.GetFieldCount()):
            value = feat.GetField(i)
            if isinstance(value, str):
                    value = unicode(value, 'latin-1')
            field = feat.GetFieldDefnRef(i)
            fieldname = field.GetName()
            mongofeat[fieldname] = value
        # insert the feature in the collection
        collection.insert(mongofeat)
        feat.Destroy()
        feat = lyr.GetNextFeature()
        k = k + 1
        pbar.update(k)
    pbar.finish()
    print '%s features loaded in MongoDb from shapefile.' % lyr.GetFeatureCount()


def mongodb2shape(mongodb_server, mongodb_port, mongodb_db, mongodb_collection, output_shape, query_filter):
    """Convert a mongodb collection (all elements must have same attributes) to a shapefile"""
    print ' Converting a mongodb collection to a shapefile '
    connection = Connection(mongodb_server, mongodb_port)
    print 'Getting database MongoDB %s...' % mongodb_db
    db = connection[mongodb_db]
    print 'Getting the collection %s...' % mongodb_collection
    collection = db[mongodb_collection]
    print 'Exporting %s elements in collection to shapefile...' % collection.count()
    drv = ogr.GetDriverByName("ESRI Shapefile")
    ds = drv.CreateDataSource(output_shape)
    lyr = ds.CreateLayer('test', None, ogr.wkbPolygon)
    print 'Shapefile %s created...' % ds.name
    cursor = collection.find(query_filter)
    # define the progressbar
    pbar = ProgressBar(collection.count()).start()
    k=0
    # iterate the features in the collection and copy them to the shapefile
    # for simplicity we export only the geometry to the shapefile
    # if we would like to store also the other fields we should have created a metadata element with fields datatype info
    for element in cursor:
        element_geom = element['geom']
        feat = ogr.Feature(lyr.GetLayerDefn())
        feat.SetGeometry(ogr.CreateGeometryFromWkt(element_geom))
        lyr.CreateFeature(feat)
        feat.Destroy()
        k = k + 1
        pbar.update(k)
    pbar.finish()
    print '%s features loaded in shapefile from MongoDb.' % lyr.GetFeatureCount()