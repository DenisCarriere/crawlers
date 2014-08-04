import geocoder
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient()
db = client['mydb']['ottawa']

# Geocode Address
g = geocoder.google('1552 Payette drive, Ottawa ON')

# Geometry in GeoJSON
geometry = dict()
geometry['type'] = 'Point'
geometry['coordinates'] = [g.lng, g.lat] 

# Spatial Query
query = db.aggregate([{'$geoNear':
	{'near': geometry,
	'distanceField':"distance",
	'limit': 5,
	'spherical': True,
	'maxDistance': 50}
}])

# View Results
for item in query['result']:
	print item['route'], item['distance']