from pymongo import MongoClient
import os
import osgeo

city = 'ottawa'
provider = 'osm'

# Connect to MongoDB
MONGO_URL = os.environ.get('COMPOSE_URL')
client = MongoClient(MONGO_URL)
db = client['geocoder']
db_city = db[city]
db_geocoder = db[provider]

# Create Shapefile
driver = ogr.GetDriverByName("ESRI Shapefile")
data = driver.CreateDataSource('test.shp')
layer = data.CreateLayer('test', None, ogr.wkbPoint)

for item in db_city.find({}).limit(5):
	print item